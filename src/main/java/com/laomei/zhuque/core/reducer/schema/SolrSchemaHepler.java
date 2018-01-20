package com.laomei.zhuque.core.reducer.schema;

import com.laomei.zhuque.util.SchemaUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Get solr collection schema
 * @author luobo
 */
public class SolrSchemaHepler implements SchemaHelper {
    private static final String SCHEMA = "schema";
    private static final String FIELDS = "fields";
    private static final String FIELD_TYPES = "fieldTypes";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String CLASS = "class";

    private String solrCollectionName;

    private SolrClient solrClient;

    public SolrSchemaHepler(@NotNull String solrCollectionName, @NotNull SolrClient solrClient) {
        this.solrCollectionName = solrCollectionName;
        this.solrClient = solrClient;
    }

    @Override
    public void init() {
    }

    @Override
    public Map<String, Class<?>> getSchema() throws Exception {
        NamedList<Object> namedList = solrClient.request(new SchemaRequest(), solrCollectionName);
        SimpleOrderedMap orderedMap = (SimpleOrderedMap) namedList.get(SCHEMA);
        List<SimpleOrderedMap> fields = (List<SimpleOrderedMap>) orderedMap.get(FIELDS);
        List<SimpleOrderedMap> fieldTypes = (List<SimpleOrderedMap>) orderedMap.get(FIELD_TYPES);
        return getSchemaMap(fields, fieldTypes);
    }

    private Map<String, Class<?>> getSchemaMap(List<SimpleOrderedMap> fields, List<SimpleOrderedMap> fieldTypes) {
        Map<String, String> fieldWithSolrType = new HashMap<>(fields.size());
        Map<String, String> fieldTypeMap = new HashMap<>(fieldTypes.size());
        Map<String, Class<?>> schemaMap = new HashMap<>(fields.size());
        fieldTypes.forEach(k -> fieldTypeMap.put(String.valueOf(k.get(NAME)), String.valueOf(k.get(CLASS))));
        fields.forEach(k -> {
            String name = String.valueOf(k.get(NAME));
            String type = String.valueOf(k.get(TYPE));
            fieldWithSolrType.put(name, fieldTypeMap.get(type));
        });
        fieldWithSolrType.forEach((field, solrType) -> {
            schemaMap.put(field, SchemaUtil.SolrSchemaType.toJavaType(solrType));
        });
        return schemaMap;
    }
}
