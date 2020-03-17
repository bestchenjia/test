package cn.xdl.myintetceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        Map<String, String> headers = event.getHeaders();
        try {
            String log = new String(body, "utf-8");
            if(log.contains("start")){
               headers.put("topic","start");
            }else {
                headers.put("topic","event");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> events = new ArrayList<>();
        for(Event event:list){
            Event intercepted = intercept(event);
            events.add(intercepted);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
