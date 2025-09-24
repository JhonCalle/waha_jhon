import {
  Inject,
  Injectable,
  NotFoundException,
  OnModuleInit,
  UnprocessableEntityException,
} from '@nestjs/common';
import {
  AppsService,
  IAppsService,
} from '@waha/apps/app_sdk/services/IAppsService';
import { EngineBootstrap } from '@waha/core/abc/EngineBootstrap';
import { GowsEngineConfigService } from '@waha/core/config/GowsEngineConfigService';
import { WebJSEngineConfigService } from '@waha/core/config/WebJSEngineConfigService';
import { WhatsappSessionGoWSCore } from '@waha/core/engines/gows/session.gows.core';
import { WebhookConductor } from '@waha/core/integrations/webhooks/WebhookConductor';
import { MediaStorageFactory } from '@waha/core/media/MediaStorageFactory';
import { DefaultMap } from '@waha/utils/DefaultMap';
import { getPinoLogLevel, LoggerBuilder } from '@waha/utils/logging';
import { promiseTimeout, sleep } from '@waha/utils/promiseTimeout';
import { complete } from '@waha/utils/reactive/complete';
import { SwitchObservable } from '@waha/utils/reactive/SwitchObservable';
import { PinoLogger } from 'nestjs-pino';
import { Observable, retry, share } from 'rxjs';
import { map } from 'rxjs/operators';

import { WhatsappConfigService } from '../config.service';
import {
  WAHAEngine,
  WAHAEvents,
  WAHASessionStatus,
} from '../structures/enums.dto';
import {
  ProxyConfig,
  SessionConfig,
  SessionDetailedInfo,
  SessionDTO,
  SessionInfo,
} from '../structures/sessions.dto';
import { WebhookConfig } from '../structures/webhooks.config.dto';
import { populateSessionInfo, SessionManager } from './abc/manager.abc';
import { SessionParams, WhatsappSession } from './abc/session.abc';
import { EngineConfigService } from './config/EngineConfigService';
import { WhatsappSessionNoWebCore } from './engines/noweb/session.noweb.core';
import { WhatsappSessionWebJSCore } from './engines/webjs/session.webjs.core';
import { getProxyConfig } from './helpers.proxy';
import { MediaManager } from './media/MediaManager';
import { LocalSessionAuthRepository } from './storage/LocalSessionAuthRepository';
import { LocalStoreCore } from './storage/LocalStoreCore';

interface SessionEntry {
  config?: SessionConfig;
  session?: WhatsappSession;
}

@Injectable()
export class SessionManagerCore extends SessionManager implements OnModuleInit {
  SESSION_STOP_TIMEOUT = 3000;

  private readonly sessions: Map<string, SessionEntry> = new Map();
  protected readonly EngineClass: typeof WhatsappSession;
  protected readonly engineBootstrap: EngineBootstrap;
  private readonly eventsBySession: DefaultMap<
    string,
    DefaultMap<WAHAEvents, SwitchObservable<any>>
  >;

  constructor(
    config: WhatsappConfigService,
    private engineConfigService: EngineConfigService,
    private webjsEngineConfigService: WebJSEngineConfigService,
    gowsConfigService: GowsEngineConfigService,
    log: PinoLogger,
    private mediaStorageFactory: MediaStorageFactory,
    @Inject(AppsService)
    appsService: IAppsService,
  ) {
    super(log, config, gowsConfigService, appsService);
    const engineName = this.engineConfigService.getDefaultEngineName();
    this.EngineClass = this.getEngine(engineName);
    this.engineBootstrap = this.getEngineBootstrap(engineName);
    this.eventsBySession = new DefaultMap(
      () =>
        new DefaultMap<WAHAEvents, SwitchObservable<any>>(() =>
          new SwitchObservable((obs$) => obs$.pipe(retry(), share())),
        ),
    );

    this.store = new LocalStoreCore(engineName.toLowerCase());
    this.sessionAuthRepository = new LocalSessionAuthRepository(this.store);
    this.clearStorage().catch((error) => {
      this.log.error({ error }, 'Error while clearing storage');
    });
  }

  protected getEngine(engine: WAHAEngine): typeof WhatsappSession {
    if (engine === WAHAEngine.WEBJS) {
      return WhatsappSessionWebJSCore;
    } else if (engine === WAHAEngine.NOWEB) {
      return WhatsappSessionNoWebCore;
    } else if (engine === WAHAEngine.GOWS) {
      return WhatsappSessionGoWSCore;
    } else {
      throw new NotFoundException(`Unknown whatsapp engine '${engine}'.`);
    }
  }

  public getSessionEvent(session: string, event: WAHAEvents): Observable<any> {
    return this.eventsBySession.get(session).get(event);
  }

  private getOrCreateEntry(name: string): SessionEntry {
    const existing = this.sessions.get(name);
    if (existing) {
      return existing;
    }
    const created: SessionEntry = {};
    this.sessions.set(name, created);
    return created;
  }

  private bindSessionEvents(name: string, session: WhatsappSession) {
    const events = this.eventsBySession.get(name);
    for (const eventName in WAHAEvents) {
      const event = WAHAEvents[eventName];
      const stream$ = session
        .getEventObservable(event)
        .pipe(map(populateSessionInfo(event, session)));
      events.get(event).switch(stream$);
    }
  }

  private unbindSessionEvents(name: string) {
    if (!this.eventsBySession.has(name)) {
      return;
    }
    const events = this.eventsBySession.get(name);
    for (const observable of events.values()) {
      observable.switch(null);
    }
  }

  private getRunningSessions(): Record<string, WhatsappSession> {
    const running: Record<string, WhatsappSession> = {};
    for (const [sessionName, entry] of this.sessions.entries()) {
      if (entry.session) {
        running[sessionName] = entry.session;
      }
    }
    return running;
  }

  async beforeApplicationShutdown(signal?: string) {
    for (const [name, entry] of this.sessions.entries()) {
      if (entry.session) {
        await this.stop(name, true);
      }
    }
    this.stopEvents();
    await this.engineBootstrap.shutdown();
  }

  async onApplicationBootstrap() {
    await this.engineBootstrap.bootstrap();
    this.startPredefinedSessions();
  }

  async exists(name: string): Promise<boolean> {
    return this.sessions.has(name);
  }

  isRunning(name: string): boolean {
    return !!this.sessions.get(name)?.session;
  }

  async upsert(name: string, config?: SessionConfig): Promise<void> {
    const entry = this.getOrCreateEntry(name);
    entry.config = config;
  }

  async start(name: string): Promise<SessionDTO> {
    const entry = this.getOrCreateEntry(name);
    if (entry.session) {
      throw new UnprocessableEntityException(
        `Session '${name}' is already started.`,
      );
    }

    this.log.info({ session: name }, `Starting session...`);
    const logger = this.log.logger.child({ session: name });
    logger.level = getPinoLogLevel(entry.config?.debug);
    const loggerBuilder: LoggerBuilder = logger;

    const storage = await this.mediaStorageFactory.build(
      name,
      loggerBuilder.child({ name: 'Storage' }),
    );
    await storage.init();

    const mediaManager = new MediaManager(
      storage,
      this.config.mimetypes,
      loggerBuilder.child({ name: 'MediaManager' }),
    );

    const webhook = new WebhookConductor(loggerBuilder);
    const proxyConfig = this.getProxyConfig(name);
    const sessionConfig: SessionParams = {
      name,
      mediaManager,
      loggerBuilder,
      printQR: this.engineConfigService.shouldPrintQR,
      sessionStore: this.store,
      proxyConfig: proxyConfig,
      sessionConfig: entry.config,
      ignore: this.ignoreChatsConfig(entry.config),
    };

    if (this.EngineClass === WhatsappSessionWebJSCore) {
      sessionConfig.engineConfig = this.webjsEngineConfigService.getConfig();
    } else if (this.EngineClass === WhatsappSessionGoWSCore) {
      sessionConfig.engineConfig = this.gowsConfigService.getConfig();
    }

    await this.sessionAuthRepository.init(name);
    // @ts-ignore
    const session = new this.EngineClass(sessionConfig);
    entry.session = session;
    this.bindSessionEvents(name, session);

    const webhooks = this.getWebhooks(entry.config);
    webhook.configure(session, webhooks);

    await this.configureApps(session);

    await session.start();
    logger.info('Session has been started.');
    return {
      name: session.name,
      status: session.status,
      config: session.sessionConfig,
    };
  }

  async stop(name: string, silent: boolean): Promise<void> {
    const entry = this.sessions.get(name);
    if (!entry?.session) {
      this.log.debug({ session: name }, `Session is not running.`);
      return;
    }

    this.log.info({ session: name }, `Stopping session...`);
    try {
      await entry.session.stop();
    } catch (err) {
      this.log.warn(`Error while stopping session '${name}'`);
      if (!silent) {
        throw err;
      }
    }
    this.log.info({ session: name }, `Session has been stopped.`);
    entry.session = undefined;
    this.unbindSessionEvents(name);
    await sleep(this.SESSION_STOP_TIMEOUT);
  }

  async unpair(name: string) {
    const session = this.sessions.get(name)?.session;
    if (!session) {
      return;
    }

    this.log.info({ session: name }, 'Unpairing the device from account...');
    await session.unpair().catch((err) => {
      this.log.warn(`Error while unpairing from device: ${err}`);
    });
    await sleep(1000);
  }

  async logout(name: string): Promise<void> {
    await this.sessionAuthRepository.clean(name);
  }

  async delete(name: string): Promise<void> {
    const entry = this.sessions.get(name);
    if (!entry) {
      return;
    }

    if (entry.session) {
      await this.stop(name, true);
    }

    this.sessions.delete(name);
    if (this.eventsBySession.has(name)) {
      const events = this.eventsBySession.get(name);
      complete(events);
      this.eventsBySession.delete(name);
    }
  }

  private getWebhooks(config?: SessionConfig) {
    let webhooks: WebhookConfig[] = [];
    if (config?.webhooks) {
      webhooks = webhooks.concat(config.webhooks);
    }
    const globalWebhookConfig = this.config.getWebhookConfig();
    if (globalWebhookConfig) {
      webhooks.push(globalWebhookConfig);
    }
    return webhooks;
  }

  protected getProxyConfig(name: string): ProxyConfig | undefined {
    const entry = this.sessions.get(name);
    if (entry?.config?.proxy) {
      return entry.config.proxy;
    }
    const sessions = this.getRunningSessions();
    return getProxyConfig(this.config, sessions, name);
  }

  getSession(name: string): WhatsappSession {
    const session = this.sessions.get(name)?.session;
    if (!session) {
      throw new NotFoundException(
        `We didn't find a session with name '${name}'.\n` +
          `Please start it first by using POST /api/sessions/${name}/start request`,
      );
    }
    return session;
  }

  async getSessions(all: boolean): Promise<SessionInfo[]> {
    const sessions: SessionInfo[] = [];
    for (const [name, entry] of this.sessions.entries()) {
      if (entry.session) {
        const session = entry.session;
        sessions.push({
          name: session.name,
          status: session.status,
          config: session.sessionConfig,
          me: session.getSessionMeInfo(),
        });
      } else if (all) {
        sessions.push({
          name: name,
          status: WAHASessionStatus.STOPPED,
          config: entry.config,
          me: null,
        });
      }
    }
    if (!all) {
      return sessions;
    }
    sessions.sort((a, b) => a.name.localeCompare(b.name));
    return sessions;
  }

  private async fetchEngineInfo(session?: WhatsappSession) {
    let engineInfo = {};
    if (session) {
      try {
        engineInfo = await promiseTimeout(1000, session.getEngineInfo());
      } catch (error) {
        this.log.debug(
          { session: session.name, error: `${error}` },
          'Can not get engine info',
        );
      }
    }
    return {
      engine: session?.engine,
      ...engineInfo,
    };
  }

  async getSessionInfo(name: string): Promise<SessionDetailedInfo | null> {
    const entry = this.sessions.get(name);
    if (!entry) {
      return null;
    }
    const session = entry.session;
    const baseInfo: SessionInfo = session
      ? {
          name: session.name,
          status: session.status,
          config: session.sessionConfig,
          me: session.getSessionMeInfo(),
        }
      : {
          name: name,
          status: WAHASessionStatus.STOPPED,
          config: entry.config,
          me: null,
        };
    const engine = await this.fetchEngineInfo(session);
    return { ...baseInfo, engine };
  }

  protected stopEvents() {
    for (const events of this.eventsBySession.values()) {
      complete(events);
    }
    this.eventsBySession.clear();
  }

  async onModuleInit() {
    await this.init();
  }

  async init() {
    await this.store.init();
    const knex = this.store.getWAHADatabase();
    await this.appsService.migrate(knex);
  }

  private async clearStorage() {
    const storage = await this.mediaStorageFactory.build(
      'all',
      this.log.logger.child({ name: 'Storage' }),
    );
    await storage.purge();
  }
}
