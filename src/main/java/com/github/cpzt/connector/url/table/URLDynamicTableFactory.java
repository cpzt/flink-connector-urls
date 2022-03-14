package com.github.cpzt.connector.url.table;

import static org.apache.flink.configuration.ConfigOptions.key;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** URLSource connector Factory. */
public class URLDynamicTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "url";

    private static final ConfigOption<String> URL =
            key("url").stringType().noDefaultValue().withDescription("The urls of web");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();
        final ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        final List<Column> physicalColumns =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList());

        if (physicalColumns.size() != 1
                || !physicalColumns
                        .get(0)
                        .getDataType()
                        .getLogicalType()
                        .getTypeRoot()
                        .equals(LogicalTypeRoot.VARCHAR)) {
            throw new ValidationException(
                    String.format(
                            "Currently, we can only read line by line. "
                                    + "That is, only one physical field of type STRING is supported, but got %d columns (%s).",
                            physicalColumns.size(),
                            physicalColumns.stream()
                                    .map(
                                            column ->
                                                    column.getName()
                                                            + " "
                                                            + column.getDataType().toString())
                                    .collect(Collectors.joining(", "))));
        }

        return new URLDynamicSource(config.get(URL), schema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
