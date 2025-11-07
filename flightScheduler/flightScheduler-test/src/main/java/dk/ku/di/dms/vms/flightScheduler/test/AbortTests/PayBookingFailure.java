package dk.ku.di.dms.vms.flightScheduler.test.AbortTests;

import dk.ku.di.dms.vms.flightScheduler.test.DataInjection;
import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsProcess;

import java.util.List;

public class PayBookingFailure
{
    public static boolean Run()
    {
        List<String> components = List.of("customer", "booking", "flight", "payment", "proxy");
        VmsProcess.KillComponents(components);
        Util.Sleep(500);
        try {
            VmsProcess.StartComponents(components);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        DataInjection.SendCustomer();

        return true;
    }
}
