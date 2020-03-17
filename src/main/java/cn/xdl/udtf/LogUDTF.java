package cn.xdl.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class LogUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        List<String> structFieldNames = new ArrayList<>();
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        structFieldNames.add("event_name");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        structFieldNames.add("event_json");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,structFieldObjectInspectors);
    }


    @Override
    public void process(Object[] args) throws HiveException {
        String input = args[0].toString();
        if (StringUtils.isBlank(input)) {
            return;
        }

        try {
            JSONArray ja = new JSONArray(input);
            if(ja == null){
                return;
            }

            for (int i = 0; i < ja.length(); i++) {
                String[] result = new String[2];
                try {
                    result[0] = ja.getJSONObject(i).getString("en");
                    result[1] = ja.getString(i);
                }catch (Exception e){
                    continue;
                }
                forward(result);
            }


        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
