package io.digdag.plugin.td;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.Proxies;
import io.digdag.util.BaseOperator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;

public class SecretsOperatorFactory implements OperatorFactory {
  private static final Logger logger = LoggerFactory.getLogger(SecretsOperatorFactory.class);

  @Nonnull
  private final TemplateEngine templateEngine;
  @Nonnull
  private final Map<String, String> env;
  @Nonnull
  private final Config systemConfig;

  @Inject
  public SecretsOperatorFactory(@CheckForNull TemplateEngine templateEngine,
      @CheckForNull @Environment Map<String, String> env, @CheckForNull Config systemConfig) {
    this.templateEngine = Objects.requireNonNull(templateEngine);
    this.env = Objects.requireNonNull(env);
    this.systemConfig = Objects.requireNonNull(systemConfig);
  }

  public String getType() {
    return "secrets";
  }

  @Override
  public Operator newOperator(OperatorContext context) {
    return new SecretsOperator(context);
  }

  private class SecretsOperator extends BaseOperator {
    public SecretsOperator(OperatorContext context) {
      super(context);
    }

    @Override
    public TaskResult runTask() {
      Config params =
          request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty("secrets"));
      SecretProvider secrets = context.getSecrets().getSecrets("td");

      Id projectId =
          Id.of(params.get("project_id", String.class, Integer.toString(request.getProjectId())));

      String endpoint = first(() -> params.getOptional("endpoint", String.class),
          () -> systemConfig.getOptional("config.td.default_endpoint", String.class),
          () -> secrets.getSecretOptional("endpoint")).or("api.treasuredata.com");

      Map<String, String> options = params.getMapOrEmpty("options", String.class, String.class);

      logger.info("Set project '" + projectId + "' secrets for endpoint: " + endpoint + "...\n"
          + options.toString());

      if (!options.isEmpty()) {
        DigdagClient client = buildClient(endpoint, env);

        for (Entry<String, String> kv : options.entrySet()) {
          client.setProjectSecret(projectId, kv.getKey(), kv.getValue());
        }
      }

      return TaskResult.empty(request);
    }
  }

  @SafeVarargs
  protected static <T> Optional<T> first(Supplier<Optional<T>>... suppliers) {
    for (Supplier<Optional<T>> supplier : suppliers) {
      Optional<T> optional = supplier.get();
      if (optional.isPresent()) {
        return optional;
      }
    }
    return Optional.absent();
  }

  @Nonnull
  static DigdagClient buildClient(@Nonnull String endpoint, @Nonnull Map<String, String> env) {
    String[] fragments = endpoint.split(":", 2);

    boolean useSsl = false;
    if (fragments.length == 2 && fragments[1].startsWith("//")) {
      // http:// or https://
      switch (fragments[0]) {
        case "http":
          useSsl = false;
          break;
        case "https":
          useSsl = true;
          break;
        default:
          throw new IllegalArgumentException(
              "Endpoint must start with http:// or https://: " + endpoint);
      }
      fragments = fragments[1].substring(2).split(":", 2);
    }

    final String host;
    final int port;
    if (fragments.length == 1) {
      host = fragments[0];
      port = useSsl ? 443 : 80;
    } else {
      host = fragments[0];
      String portString = fragments[1].split("/", 2)[0];
      port = Integer.parseInt(portString);
    }

    String scheme = useSsl ? "https" : "http";
    logger.debug("Using endpoint {}://{}:{}", scheme, host, port);

    DigdagClient.Builder builder = DigdagClient.builder().host(host).port(port).ssl(useSsl);

    Optional<ProxyConfig> proxyConfig = Proxies.proxyConfigFromEnv(scheme, env);
    if (proxyConfig.isPresent()) {
      ProxyConfig cfg = proxyConfig.get();
      if (cfg.getUser().isPresent() || cfg.getPassword().isPresent()) {
        logger
            .warn("HTTP proxy authentication not supported. Ignoring proxy username and password.");
      }
      builder.proxyHost(cfg.getHost());
      builder.proxyPort(cfg.getPort());
      builder.proxyScheme(cfg.useSSL() ? "https" : "http");
    }

    return builder.build();
  }

}
