package dk.ku.di.dms.vms.sdk.core.operational;

public record InboundEvent (
        long tid, long lastTid, long batch, String event, Class<?> clazz, Object input, int aborted)
{
    @Override
    public String toString() {
        return "{"
                + "\"batch\":\"" + batch + "\""
                + ",\"tid\":\"" + tid + "\""
                + ",\"lastTid\":\"" + lastTid + "\""
                + ",\"event\":\"" + event + "\""
                + ",\"clazz\":" + clazz
                + ",\"input\":" + input
                + ",\"aborted\":" + aborted
                + "}";
    }
}
