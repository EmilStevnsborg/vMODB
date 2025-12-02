package dk.ku.di.dms.vms.sdk.core.operational;

public record InboundEvent (
        long tid, long lastTid, long batch, long generation, String event, Class<?> clazz, Object input)
{
    @Override
    public String toString() {
        return "{"
                + "\"batch\":\"" + batch + "\""
                + ",\"tid\":\"" + tid + "\""
                + ",\"generation\":\"" + generation + "\""
                + ",\"lastTid\":\"" + lastTid + "\""
                + ",\"event\":\"" + event + "\""
                + ",\"clazz\":" + clazz
                + ",\"input\":" + input
                + "}";
    }
}
