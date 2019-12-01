/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.multicast;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.cluster.TestClusterMembershipService;
import io.atomix.protocols.multicast.impl.DefaultGenericMulticastServer;
import io.atomix.protocols.multicast.protocol.GenericMulticastProtocolFactory;
import io.atomix.protocols.multicast.service.GenericMulticastSingletonAtomixClient;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.serializers.DefaultSerializers;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class GenericMulticastTest extends ConcurrentTestCase {
  private GenericMulticastProtocolFactory protocolFactory;
  protected volatile int nextId;
  protected volatile Atomix atomix;
  private volatile int sessionId;
  private PrimaryElection election;
  protected volatile List<MemberId> nodes;
  protected volatile List<GenericMulticastServer> servers;
  protected volatile List<GenericMulticastClient> clients;

  /**
   * Test commands
   */

  @Test
  public void testCommandOneServer() throws Exception {
    testSubmitCommandSequence(1, ReadConsistency.PROTOCOL, 20);
  }

  @Test
  public void testCommandThreeServers() throws Exception {
    testSubmitCommandSequence(3, ReadConsistency.PROTOCOL, 10);
  }

  private void testSubmitCommandSequence(int nodes, ReadConsistency readConsistency, int commands) throws Exception {
    createServers(nodes);

    GenericMulticastClient client = createClient();
    SessionClient session = createProxy(client, readConsistency);

    for (int i = 0; i < commands; i++) {
      session.execute(PrimitiveOperation.operation(WRITE, SERIALIZER.encode(i)))
          .thenRun(this::resume);
      await(5_000, TimeUnit.SECONDS);
    }

  }

  /**
   * Test queries
   */

  @Test
  public void testOneNodeQuery() throws Throwable {
    testSubmitQuery(1, ReadConsistency.PROTOCOL);
  }

  @Test
  public void testThreeNodesQuery() throws Throwable {
    testSubmitQuery(3, ReadConsistency.PROTOCOL);
  }

  private void testSubmitQuery(int nodes, ReadConsistency readConsistency) throws Exception {
    createServers(nodes);

    GenericMulticastClient client = createClient();
    SessionClient session = createProxy(client, readConsistency);
    session.execute(PrimitiveOperation.operation(READ))
        .thenRun(this::resume);

    await(30_000);
  }

  @Test
  public void testOneServerEvents() throws Throwable {
    testEvents(1, ReadConsistency.PROTOCOL);
  }

  @Test
  public void testThreeServersEvents() throws Throwable {
    testEvents(3, ReadConsistency.PROTOCOL);
  }

  private void testEvents(int nodes, ReadConsistency readConsistency) throws Throwable {
    createServers(nodes);
    AtomicLong count = new AtomicLong();
    AtomicLong index = new AtomicLong();

    GenericMulticastClient client = createClient();
    SessionClient session = createProxy(client, readConsistency);
    session.addEventListener(CHANGE_EVENT, event -> {
      threadAssertEquals(count.incrementAndGet(), 2L);
      threadAssertEquals(index.get(), SERIALIZER.decode(event.value()));
      resume();
    });

    session.execute(PrimitiveOperation.operation(EVENT, SERIALIZER.encode(true)))
        .<Long>thenApply(SERIALIZER::decode)
        .thenAccept(result -> {
          threadAssertNotNull(result);
          threadAssertEquals(count.incrementAndGet(), 1L);
          index.set(result);
          resume();
        });

    await(30_000, 2);
  }

  @Test
  public void testOneServeManySessionsManyEvents() throws Throwable {
    testManySessionsManyEvents(1, ReadConsistency.PROTOCOL);
  }

  @Test
  public void testThreeServersManySessionsManyEvents() throws Throwable {
    testManySessionsManyEvents(3, ReadConsistency.COMMIT);
  }

  private void testManySessionsManyEvents(int servers, ReadConsistency readConsistency) throws Throwable {
    createServers(servers);

    SessionClient session = createProxy(createClient(), readConsistency);
    session.addEventListener(CHANGE_EVENT, event -> {
      threadAssertNotNull(event);
      resume();
    });

    SessionClient session1 = createProxy(createClient(), readConsistency);
    session1.execute(PrimitiveOperation.operation(READ)).thenRun(this::resume);
    session1.addEventListener(CHANGE_EVENT, event -> {
      threadAssertNotNull(event);
      resume();
    });

    SessionClient session2 = createProxy(createClient(), readConsistency);
    session2.execute(PrimitiveOperation.operation(READ)).thenRun(this::resume);
    session2.addEventListener(CHANGE_EVENT, event -> {
      threadAssertNotNull(event);
      resume();
    });

    await(30_000, 2);

    for (int i = 0; i < 10; i++) {
      session.execute(PrimitiveOperation.operation(EVENT, SERIALIZER.encode(false))).thenRun(this::resume);
      await(30_000, 4);
    }
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private MemberId nextNodeId() {
    return MemberId.from(String.valueOf(++nextId));
  }

  /**
   * Returns the next unique session identifier.
   */
  private SessionId nextSessionId() {
    return SessionId.from(++sessionId);
  }

  private List<GenericMulticastServer> createServers(int count) throws TimeoutException {
    List<GenericMulticastServer> servers = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      nodes.add(nextNodeId());
      GenericMulticastServer server = createServer(nodes.get(i));
      server.start().thenRun(this::resume);
      servers.add(server);
    }

    await(5000, count);

    return servers;
  }

  /**
   * Creates generic multicast server
   *
   * @param memberId: server member id
   * @return generic multicast server
   */
  private GenericMulticastServer createServer(MemberId memberId) {
    GenericMulticastServer server = DefaultGenericMulticastServer.builder()
        .withServerName("test")
        .withProtocol(protocolFactory.newServerProtocol(memberId))
        .withMembershipService(new TestClusterMembershipService(memberId, nodes))
        .withMemberGroupProvider(MemberGroupStrategy.NODE_AWARE)
        .withTimestampProvider(atomix.getAtomicCounter("gmcast-atomic-counter"))
        .withPrimaryElection(election)
        .build();
    servers.add(server);
    return server;
  }

  /**
   * Create generic multicast client
   * @return generic multicast client
   */
  private GenericMulticastClient createClient() {
    MemberId memberId = nextNodeId();
    GenericMulticastClient client = GenericMulticastClient.builder()
        .withClientName("test")
        .withPartitionId(PartitionId.from("test", 1))
        .withMembershipService(new TestClusterMembershipService(memberId, nodes))
        .withSessionIdProvider(() -> CompletableFuture.completedFuture(nextSessionId()))
        .withProtocol(protocolFactory.newClientProtocol(memberId))
        .build();
    clients.add(client);
    return client;
  }

  private SessionClient createProxy(GenericMulticastClient client, ReadConsistency readConsistency) {
    try {
      return client.sessionBuilder("generic-multicast-test", TestPrimitiveType.INSTANCE, new ServiceConfig())
          .withRecoveryStrategy(Recovery.RECOVER)
          .withReadConsistency(readConsistency)
          .build()
          .connect()
          .get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  @After
  public void tearDown() throws Exception {
    String[] members = {"localhost:15678"};
    atomix = GenericMulticastSingletonAtomixClient.instance(15678, members)
        .atomix();
    nextId = 0;
    sessionId = 0;
    nodes = new ArrayList<>();
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    removeFolders(".data");
    protocolFactory = new GenericMulticastProtocolFactory();
    election = new TestPrimaryElection(PartitionId.from("test", 1));
    atomix.start().get(30, TimeUnit.SECONDS);
    Futures.allOf(servers.stream()
        .map(s -> s.stop().exceptionally(v -> null))
        .collect(Collectors.toList()))
        .get(30, TimeUnit.SECONDS);
    Futures.allOf(clients.stream()
        .map(c -> c.close().exceptionally(v -> null))
        .collect(Collectors.toList()))
        .get(30, TimeUnit.SECONDS);
  }

  private void removeFolders(String prefix) throws IOException {
    Path directory = Paths.get(prefix);

    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }

  private static final Serializer SERIALIZER = DefaultSerializers.BASIC;
  private static final OperationId WRITE = OperationId.command("write");
  private static final OperationId EVENT = OperationId.command("event");
  private static final OperationId EXPIRE = OperationId.command("expire");
  private static final OperationId CLOSE = OperationId.command("close");

  private static final OperationId READ = OperationId.query("read");

  private static final EventType CHANGE_EVENT = EventType.from("change");
  private static final EventType EXPIRE_EVENT = EventType.from("expire");
  private static final EventType CLOSE_EVENT = EventType.from("close");

  public static class TestPrimitiveType implements PrimitiveType {
    private static final TestPrimitiveType INSTANCE = new TestPrimitiveType();

    @Override
    public String name() {
      return "generic-multicast-test";
    }

    @Override
    public PrimitiveConfig newConfig() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveBuilder newBuilder(String primitiveName, PrimitiveConfig config, PrimitiveManagementService managementService) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveService newService(ServiceConfig config) {
      return new TestPrimitiveService();
    }
  }

  /**
   * Test state machine.
   */
  public static class TestPrimitiveService extends AbstractPrimitiveService<Object> {
    private Commit<Void> expire;
    private Commit<Void> close;

    public TestPrimitiveService() {
      super(TestPrimitiveType.INSTANCE);
    }

    @Override
    public Serializer serializer() {
      return SERIALIZER;
    }

    @Override
    protected void configure(ServiceExecutor executor) {
      executor.register(WRITE, this::write);
      executor.register(READ, this::read);
      executor.register(EVENT, this::event);
      executor.register(CLOSE, (Consumer<Commit<Void>>) this::close);
      executor.register(EXPIRE, (Consumer<Commit<Void>>) this::expire);
    }

    @Override
    public void onExpire(Session session) {
      if (expire != null) {
        expire.session().publish(EXPIRE_EVENT);
      }
    }

    @Override
    public void onClose(Session session) {
      if (close != null && !session.equals(close.session())) {
        close.session().publish(CLOSE_EVENT);
      }
    }

    @Override
    public void backup(BackupOutput writer) {
      writer.writeLong(10);
    }

    @Override
    public void restore(BackupInput reader) {
      assertEquals(10, reader.readLong());
    }

    protected long write(Commit<Void> commit) {
      return commit.index();
    }

    protected long read(Commit<Void> commit) {
      return commit.index();
    }

    protected long event(Commit<Boolean> commit) {
      if (commit.value()) {
        commit.session().publish(CHANGE_EVENT, commit.index());
      } else {
        for (Session session : getSessions()) {
          session.publish(CHANGE_EVENT, commit.index());
        }
      }
      return commit.index();
    }

    public void close(Commit<Void> commit) {
      this.close = commit;
    }

    public void expire(Commit<Void> commit) {
      this.expire = commit;
    }
  }
}
