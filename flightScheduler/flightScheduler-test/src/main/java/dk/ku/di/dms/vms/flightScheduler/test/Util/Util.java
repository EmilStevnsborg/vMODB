package dk.ku.di.dms.vms.flightScheduler.test.Util;

public class Util
{
    public static void Sleep(int milliseconds)
    {
        try
        {
            System.out.println(STR."Sleeping for \{milliseconds}ms....");
            Thread.sleep(milliseconds);
        } catch (InterruptedException e){}
    }
}
