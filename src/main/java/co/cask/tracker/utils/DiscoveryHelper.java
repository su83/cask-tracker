/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tracker.utils;

import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Creates discovery and zookeeper clients to be used by all helpers
 */
public class DiscoveryHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DiscoveryHelper.class);
  private static final int BASEDELAY = 500;
  private static final int MAXDELAY = 2000;

  public static DiscoveryMetadataClient createMetadataClient(HttpServiceRequest request, String zookeeperQuorum)
    throws UnauthorizedException {
    try {
      String hostport = Objects.firstNonNull(request.getHeader("host"), request.getHeader("Host"));
      LOG.info("Creating ConnectionConfig using host and port {}", hostport);
      String hostName = hostport.split(":")[0];
      int port = Integer.parseInt(hostport.split(":")[1]);
      ConnectionConfig connectionConfig = ConnectionConfig.builder()
        .setHostname(hostName)
        .setPort(port)
        .build();
      ClientConfig config = ClientConfig.builder().setConnectionConfig(connectionConfig).build();
      try {
        new MetaClient(config).ping();
      } catch (IOException e) {
        config = ClientConfig.getDefault();
        LOG.debug("Got error while pinging router. Falling back to default client config: " + config, e);
      }
      return new DiscoveryMetadataClient(config);

      // create it based upon ClientConfig if you don't get an exception
    } catch (UnauthenticatedException e) {
      // Authentication is enabled, so we can't go through router. Have to use discovery via zookeeper.
      // Note that we can't use zookeeper discovery in CDAP standalone.
      LOG.debug("Got error while pinging router. Falling back to DiscoveryMetadataClient.", e);
      LOG.info("Using discovery with zookeeper quorum {}", zookeeperQuorum);
      //delete "kafka" to make "/cdap/kafka" to "/cdap"
      ZKClientService zkClient = createZKClient(zookeeperQuorum.replace("/kafka", ""));
      zkClient.startAndWait();
      ZKDiscoveryService zkDiscoveryService = new ZKDiscoveryService(zkClient);
      return new DiscoveryMetadataClient(zkDiscoveryService);
    }
  }

  private static ZKClientService createZKClient(String zookeeperQuorum) {
    Preconditions.checkNotNull(zookeeperQuorum, "Missing ZooKeeper configuration '%s'", Constants.Zookeeper.QUORUM);
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(zookeeperQuorum)
            .build(),
          RetryStrategies.exponentialDelay(BASEDELAY, MAXDELAY, TimeUnit.MILLISECONDS)
        )
      )
    );
  }

}
