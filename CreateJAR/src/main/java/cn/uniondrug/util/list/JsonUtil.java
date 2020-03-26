package cn.uniondrug.util.list;

import com.alibaba.fastjson.JSONObject;

/**
 * @author ：gjq
 * @motto ：不忘初心
 * @date ：Created in 2020/3/24 21:53
 * @Function :
 */
public class JsonUtil {

    public JSONObject  getObj(String ds){

        JSONObject jsonObject = new JSONObject();
        JSONObject emp = jsonObject.getJSONObject(ds);

        /*Map<String, String> map = new HashMap<String, String>();
        map.put("id",emp.getString("id"));
        map.put("username",emp.getString("username"));
        map.put("password",emp.getString("password"));
        map.put("gmtUpdated",emp.getString("gmtUpdated"));*/

        return emp;
    }
}
