package org.apache.beam.sdk.extensions.scripting;

import java.util.List;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScriptingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ScriptingParDo.class);

  static ScriptEngine getScriptingEngine(String language) {
    final ScriptEngineManager manager =
        new ScriptEngineManager(Thread.currentThread().getContextClassLoader());

    ScriptEngine engine = manager.getEngineByExtension(language);
    if (engine == null) {
      engine = manager.getEngineByName(language);
      if (engine == null) {
        engine = manager.getEngineByMimeType(language);
      }
    }

    if (engine == null) {
      throw new IllegalArgumentException(
          "Language ["
              + language
              + "] "
              + "not supported. Check that the needed dependencies are configured.");
    }

    return engine;
  }

  static CompiledScript compileScript(final ScriptEngine engine, final String script) {
    if (Compilable.class.isInstance(engine)) {
      try {
        return Compilable.class.cast(engine).compile(script);
      } catch (ScriptException e) {
        throw new IllegalStateException(e);
      }
    } else {
      return new CompiledScript() {
        @Override
        public Object eval(final ScriptContext context) throws ScriptException {
          return engine.eval(script, context);
        }

        @Override
        public ScriptEngine getEngine() {
          return engine;
        }
      };
    }
  }

  static void logScriptEngines() {
    final ScriptEngineManager manager =
        new ScriptEngineManager(Thread.currentThread().getContextClassLoader());

    List<ScriptEngineFactory> engineFactories = manager.getEngineFactories();
    for (ScriptEngineFactory factory : engineFactories) {
      LOG.debug("----");
      LOG.debug("Name : " + factory.getEngineName());
      LOG.debug("Version : " + factory.getEngineVersion());
      LOG.debug("Language name : " + factory.getLanguageName());
      LOG.debug("Language version : " + factory.getLanguageVersion());
      LOG.debug("Extensions : " + factory.getExtensions());
      LOG.debug("Mime types : " + factory.getMimeTypes());
      LOG.debug("Names : " + factory.getNames());
    }
  }
}
