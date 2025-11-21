package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;


import dk.ku.di.dms.vms.flightScheduler.test.DataGenerator;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;

import java.io.IOException;
import java.net.http.HttpClient;

// send transactions
// commit
// crash a VMS
// restart
// query from endpoint
public class VmsReloadPersistentData
{
    public static boolean Run(HttpClient client) throws IOException {

        ComponentProcess.KillVMSes();
        ComponentProcess.Kill("proxy");
        System.console().readLine();

        try {
            ComponentProcess.StartVMSes();
            ComponentProcess.StartProxy(false, 1, 1, 1,
                    Integer.MAX_VALUE, 10);
        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // wait for components to come online
        System.console().readLine();
        System.out.println("TEST: injecting data");

        var customers = DataGenerator.GenerateCustomers(client, 40);
        var flightSeats = DataGenerator.GenerateFlightSeats(client, 0, 40);

        var success = true;
        return success;
    }
}
