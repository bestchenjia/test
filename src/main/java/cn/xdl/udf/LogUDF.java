package cn.xdl.udf;



import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class LogUDF extends UDF {
    public String evaluate(String line,String jsonkey){
        String[] jsonkeys = jsonkey.split(",");
        String[] split = line.split("\\|");
        if(split.length!=2|| StringUtils.isBlank(split[1])){
            return "";
        }
        String date = split[0];
        String jsonstring = split[1];

        StringBuilder sb = new StringBuilder();
        try {
            JSONObject jsonObject = new JSONObject(jsonstring);
            JSONObject cm = jsonObject.getJSONObject("cm");
            for (int i = 0; i <jsonkeys.length ; i++) {
                if(cm.has(jsonkeys[i])){
                    String value = cm.getString(jsonkeys[i].trim());
                    sb.append(value).append("\t");
                }else {
                    sb.append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(split[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    };
}
