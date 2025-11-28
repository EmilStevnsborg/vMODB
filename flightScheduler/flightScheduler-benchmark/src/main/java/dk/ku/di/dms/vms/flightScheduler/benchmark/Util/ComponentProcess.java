package dk.ku.di.dms.vms.flightScheduler.benchmark.Util;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ComponentProcess
{
    private static ProcessBuilder CreateProcessBuilder(String jarPath,
                                                       List<String> args)
    {
        String javaBin = STR."\{System.getProperty("java.home")}\{File.separator}bin\{File.separator}java";

        var processBuilder = new ProcessBuilder(
                javaBin,
                "--enable-preview",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens", "java.base/jdk.internal.util=ALL-UNNAMED",
                "-jar",
                jarPath
        );
        processBuilder.command().addAll(args);

        processBuilder.directory(new File("../flightScheduler-customer"));

        processBuilder.inheritIO();
        return processBuilder;
    }

    public static void Kill(String vmsIdentifier)
    {
        var vms = STR."flightScheduler-\{vmsIdentifier}";
        ProcessHandle.allProcesses()
                .filter(ph -> ph.info().commandLine().isPresent())
                .filter(ph -> ph.info().commandLine().get().contains(vms))
                .forEach(ph -> {
                    ph.destroy();
                    while (ph.isAlive()) {}
                });

        System.out.println(STR."\{vmsIdentifier} killed");
    }
    public static ProcessBuilder VmsProcessBuilder(String vmsIdentifier)
    {
        return ComponentProcess.VmsProcessBuilder(vmsIdentifier, List.of());
    }
    public static ProcessBuilder VmsProcessBuilder(String vmsIdentifier, List<String> args)
    {
        var jarPath = STR."../flightScheduler-\{vmsIdentifier}/target/flightScheduler-\{vmsIdentifier}-1.0-SNAPSHOT-jar-with-dependencies.jar";
        var processBuilder = CreateProcessBuilder(jarPath, args);

        File workingDir = new File(STR."../flightScheduler-\{vmsIdentifier}");
        processBuilder.directory(workingDir);

        return processBuilder;
    }

    public static void KillComponents(List<String> components)
    {
        for (var component : components)
        {
            ComponentProcess.Kill(component);
        }
    }
    public static void KillVMSes()
    {
        List<String> components = List.of("customer", "booking", "flight", "payment");
        KillComponents(components);
    }

    public static void StartComponents(List<String> components) throws IOException
    {
        for (var component : components)
        {
            ComponentProcess.VmsProcessBuilder(component, List.of()).start();
        }
    }

    public static void StartVms(String vms, boolean recoverable, int numVmsWorkers) throws IOException
    {
        var args = List.of(STR."recoverable=\{recoverable}", STR."num_vms_workers=\{numVmsWorkers}");
        ComponentProcess.VmsProcessBuilder(vms, args).start();
    }
    public static void StartVMSes() throws Exception
    {
        List<String> components = List.of("customer", "booking", "flight", "payment");
        ComponentProcess.StartComponents(components);
    }
    public static void StartProxy(boolean recoverable, int numVmsWorkers, int numQueuesVmsWorker,
                                  int numTransactionWorkers, int timeout, int maxTransactionsPerBatch)
            throws IOException
    {
        var args = List.of(STR."recoverable=\{recoverable}",
                           STR."n1um_vms_workers=\{numVmsWorkers}",
                           STR."num_queues_vms_worker=\{numQueuesVmsWorker}",
                           STR."num_transaction_workers=\{numTransactionWorkers}",
                           STR."batch_window_ms=\{timeout}",
                           STR."num_max_transactions_batch=\{maxTransactionsPerBatch}");
        ComponentProcess.VmsProcessBuilder(
                "proxy",
                args
        ).start();

        System.out.println(STR."STARTED PROXY with args: \{String.join(",", args)}");
    }
}
