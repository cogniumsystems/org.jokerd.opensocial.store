/**
 * 
 */
package org.jokerd.opensocial.store;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ubimix.commons.json.IJsonAccessor.JsonType;
import org.ubimix.commons.json.JsonAccessor;
import org.ubimix.commons.json.JsonArray;
import org.ubimix.commons.json.JsonObject;
import org.ubimix.commons.json.JsonValue.IJsonValueFactory;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author kotelnikov
 */
public class JsonStore {

    public interface ITypeProvider {
        String getFieldType(JsonObject obj, String path);
    }

    public static JsonStore open(File dir) {
        dir.getParentFile().mkdirs();
        OGraphDatabase database = new OGraphDatabase("local:" + dir);
        if (!database.exists()) {
            database.create();
        } else {
            database.open("admin", "admin");
        }
        return new JsonStore(database);
    }

    private final OCommandRequest fCmdGetDocById;

    private final OGraphDatabase fDatabase;

    public JsonStore(OGraphDatabase db) {
        fDatabase = db;
        OSQLSynchQuery<ODocument> syncQuery = new OSQLSynchQuery<ODocument>(
            "select from Json where id = :id");
        fCmdGetDocById = fDatabase.command(syncQuery);
        initializeTypes();
    }

    public void close() {
        fDatabase.close();
    }

    public OGraphDatabase getDatabase() {
        return fDatabase;
    }

    private ODocument getDocById(String id) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("id", id);
        List<ODocument> result = fCmdGetDocById.execute(params);
        return !result.isEmpty() ? result.get(0) : null;
    }

    private Object getDocValue(
        final JsonObject root,
        final String path,
        Object value,
        final ITypeProvider types) {
        Object result = null;
        JsonType t = JsonAccessor.getInstance().getType(value);
        if (t == JsonType.OBJECT || t == JsonType.ARRAY) {
            if (t == JsonType.OBJECT) {
                JsonObject child = JsonObject.FACTORY.newValue(value);
                ODocument childDoc = save(root, path, child, types);
                result = childDoc;
            } else if (t == JsonType.ARRAY) {
                JsonArray array = JsonArray.FACTORY.newValue(value);
                List<Object> values = array
                    .getList(new IJsonValueFactory<Object>() {
                        @Override
                        public Object newValue(Object object) {
                            return getDocValue(root, path, object, types);
                        }
                    });
                result = values;
            }
        } else {
            result = value;
        }
        return result;
    }

    protected OClass getOrCreateClass(String name, OClass parentCls) {
        OSchema schema = fDatabase.getMetadata().getSchema();
        OClass cls = schema.getClass(name);
        if (cls == null) {
            cls = schema.createClass(name);
            if (parentCls != null) {
                cls.setSuperClass(parentCls);
            }
        }
        return cls;
    }

    protected OClass getRootType() {
        OClass jsonType = getOrCreateClass("Json", null);
        return jsonType;
    }

    private void initializeTypes() {
        getRootType();
    }

    public JsonObject loadById(String id) {
        ODocument doc = getDocById(id);
        JsonObject result = null;
        if (doc != null) {
            result = toJson(doc);
        }
        return result;
    }

    public ODocument save(JsonObject obj, ITypeProvider types) {
        return save(obj, "", obj, types);
    }

    private ODocument save(
        JsonObject rootObj,
        String path,
        JsonObject obj,
        ITypeProvider types) {
        String id = obj.getString("id");
        String type = types.getFieldType(rootObj, path);
        ODocument doc = null;
        if (id != null) {
            doc = getDocById(id);
        }
        if (doc != null) {
            return doc;
        }
        doc = new ODocument();
        OClass rootClss = getRootType();
        if (type != null) {
            getOrCreateClass(type, rootClss);
        } else if (id != null) {
            type = rootClss.getName();
        }
        if (type != null) {
            doc.setClassName(type);
        }
        for (String key : obj.getKeys()) {
            Object value = obj.getValue(key);
            String childPath = "";
            if (!"".equals(path)) {
                childPath += ".";
            }
            childPath += key;
            Object docValue = getDocValue(rootObj, childPath, value, types);
            doc.field(key, docValue);
        }
        fDatabase.save(doc);
        return doc;
    }

    public JsonObject toJson(ODocument d) {
        return toJson(d, new HashSet<ORID>());
    }

    private JsonObject toJson(ODocument d, Set<ORID> set) {
        ORID id = d.getIdentity();
        if (set.contains(id)) {
            return null;
        }
        set.add(id);
        JsonObject obj = new JsonObject();
        String[] fieldNames = d.fieldNames();
        for (String field : fieldNames) {
            Object value = d.field(field);
            if (value instanceof ODocument) {
                value = toJson((ODocument) value, set);
            }
            obj.setValue(field, value);
        }
        return obj;
    }

}
