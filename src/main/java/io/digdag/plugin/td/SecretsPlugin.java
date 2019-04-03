package io.digdag.plugin.td;

import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;
import io.digdag.spi.TemplateEngine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

public class SecretsPlugin implements Plugin {
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return SecretsOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    public static class SecretsOperatorProvider implements OperatorProvider {
        @Inject
        protected TemplateEngine templateEngine;
        
        @Inject
        @Environment
        protected Map<String, String> env;
        
        @Inject
        protected Config systemConfig;

        @Override
        public List<OperatorFactory> get() {
            return Arrays.asList(new SecretsOperatorFactory(templateEngine, env, systemConfig));
        }
    }
}
