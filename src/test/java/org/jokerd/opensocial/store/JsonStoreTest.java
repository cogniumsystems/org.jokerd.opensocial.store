/**
 * 
 */
package org.jokerd.opensocial.store;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.jokerd.opensocial.store.JsonStore.ITypeProvider;
import org.ubimix.commons.io.IOUtil;
import org.ubimix.commons.json.JsonObject;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author kotelnikov
 */
public class JsonStoreTest extends TestCase {

    /**
     * @param name
     */
    public JsonStoreTest(String name) {
        super(name);
    }

    public void test() throws Exception {
        try {
            File dir = new File("./tmp/graphdb");
            IOUtil.delete(dir);
            dir.getParentFile().mkdirs();
            JsonStore store = JsonStore.open(dir);
            try {
                JsonObject obj = JsonObject
                    .newValue("{\n"
                        + "  \"id\":\"tweeter.com:_3190184561785843712-X\",\n"
                        + "  \"published\":\"2012-04-11T21:08:27+0000\",\n"
                        + "  \"title\":\"Reading the Tea Leaves in the Google+ Redesign  http://t.co/y3sy0aLQ\",\n"
                        + "  \"actor\":{\n"
                        + "    \"id\":\"tweeter.com:actor-ReadWriteWeb\",\n"
                        + "    \"objectType\":\"person\",\n"
                        + "    \"displayName\":\"ReadWriteWeb\",\n"
                        + "    \"content\":\"The latest news, analysis and conversation in all things web, tech and social media from the ReadWriteWeb.com team.\",\n"
                        + "    \"url\":\"http://www.readwriteweb.com\",\n"
                        + "    \"published\":\"2007-04-14T22:48:21+0000\",\n"
                        + "    \"image\":{\n"
                        + "      \"url\":\"http://a0.twimg.com/profile_images/1112101815/rwwlogo_twitter_normal.png\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"verb\":\"post\",\n"
                        + "  \"object\":{\n"
                        + "    \"id\":\"tweeter.com:_3190184561785843712\",\n"
                        + "    \"objectType\":\"feed-entry\",\n"
                        + "    \"published\":\"2012-04-11T21:08:27+0000\",\n"
                        + "    \"updated\":\"2012-04-11T21:08:27+0000\",\n"
                        + "    \"url\":\"http://t.co/y3sy0aLQ\",\n"
                        + "    \"displayName\":\"Reading the Tea Leaves in the Google+ Redesign  http://t.co/y3sy0aLQ\",\n"
                        + "    \"content\":\"Reading the Tea Leaves in the Google+ Redesign  <a href='http://t.co/y3sy0aLQ'>http://t.co/y3sy0aLQ</a>\"\n"
                        + "  },\n"
                        + "  \"target\":{\n"
                        + "    \"id\":\"tweeter.com:ReadWriteWeb\",\n"
                        + "    \"objectType\":\"twitter\",\n"
                        + "    \"displayName\":\"twitter: ReadWriteWeb\"\n"
                        + "  }\n"
                        + "}");
                final Map<String, String> types = new HashMap<String, String>();
                types.put("", "ActivityEntry");
                types.put("actor", "ActivityObject");
                types.put("object", "ActivityObject");
                types.put("target", "ActivityObjectRef");
                store.save(obj, new ITypeProvider() {
                    @Override
                    public String getFieldType(JsonObject obj, String path) {
                        return types.get(path);
                    }
                });

                OGraphDatabase database = store.getDatabase();
                List<ODocument> result = database
                    .query(new OSQLSynchQuery<ODocument>(
                        "select * from Json where id = 'tweeter.com:_3190184561785843712'"));
                for (ODocument d : result) {
                    JsonObject json = store.toJson(d);
                    System.out.println(json);
                }

                JsonObject testJson = store
                    .loadById("tweeter.com:_3190184561785843712-X");
                assertEquals(obj, testJson);

                OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
                    "select from ActivityEntry");
                Map<String, Object> params = new HashMap<String, Object>();
                params.put("actorName", "%ReadWriteWeb%");
            } finally {
                store.close();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
