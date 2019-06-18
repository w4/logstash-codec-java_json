package net.doyl.logstash.codec.json;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

@ThreadSafe
@LogstashPlugin(name="java_json")
public class JavaJson implements Codec {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ThreadLocal<Map<String, Object>> READER_MAP = ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<ObjectReader> JSON_READER = ThreadLocal.withInitial(
        () -> JSON_MAPPER.readerForUpdating(READER_MAP.get())
    );

    private final String id;
    private final Logger log;
    private final Context context;

    public JavaJson(final Configuration config, final Context context) {
        this(context);
    }

    private JavaJson(final Context context) {
        this.id = UUID.randomUUID().toString();
        this.log = context.getLogger(this);
        this.context = context;
    }

    @Override
    public void decode(final ByteBuffer byteBuffer, final Consumer<Map<String, Object>> consumer) {
        try {
            final InputStream bufferInputStream = new ByteBufferBackedInputStream(byteBuffer);
            final Map<String, Object> map = JSON_READER.get().readValue(bufferInputStream);
            consumer.accept(map);
            map.clear();
        } catch (final IOException e) {
            log.fatal("Failed to parse JSON object.", e);
            READER_MAP.get().clear();
        }
    }

    @Override
    public void flush(final ByteBuffer byteBuffer, final Consumer<Map<String, Object>> consumer) {
        this.decode(byteBuffer, consumer);
    }

    // @Override
    public void encode(final Event event, final OutputStream outputStream) throws IOException {
        JSON_MAPPER.writeValue(outputStream, event.getData());
    }

    public boolean encode(final Event event, ByteBuffer buffer) {
        throw new UnsupportedOperationException("encode is not supported on logstash versions < 7.2");
    }

    @Override
    public Codec cloneCodec() {
        return new JavaJson(this.context);
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return ImmutableSet.of();
    }

    @Override
    public String getId() {
        return this.id;
    }
}
