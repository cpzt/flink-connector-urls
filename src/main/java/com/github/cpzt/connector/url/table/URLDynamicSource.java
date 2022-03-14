package com.github.cpzt.connector.url.table;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.github.cpzt.connector.url.source.URLSource;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

/** */
public class URLDynamicSource implements ScanTableSource {

    private final URL[] urls;
    private final ResolvedSchema schema;

    public URLDynamicSource(String url, ResolvedSchema schema) {
        this(Stream.of(checkNotNull(url)).map(x -> {
            try {
                return new URL(url);
            } catch (MalformedURLException e) {
                throw new FlinkRuntimeException(e.getMessage());
            }
        }).toArray(URL[]::new), schema);
    }

    public URLDynamicSource(URL[] urls, ResolvedSchema schema) {
        this.urls = checkNotNull(urls);
        this.schema = checkNotNull(schema);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(schema.toPhysicalRowDataType());

        return SourceProvider.of(new URLSource(producedTypeInfo, urls));
    }

    @Override
    public DynamicTableSource copy() {
        return new URLDynamicSource(urls, schema);
    }

    @Override
    public String asSummaryString() {
        return "URLSource";
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        URLDynamicSource that = (URLDynamicSource) o;
        return Arrays.equals(urls, that.urls) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(schema);
        result = 31 * result + Arrays.hashCode(urls);
        return result;
    }

    @Override
    public String toString() {
        return "URLDynamicSource{"
                + "paths="
                + Arrays.toString(urls)
                + ", schema="
                + schema
                + '}';
    }
}
