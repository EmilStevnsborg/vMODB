package dk.ku.di.dms.vms.flightScheduler.test.Util;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class VmsProcess
{
    private static ProcessBuilder CreateProcessBuilder(String jarPath, boolean recoverEnabled, List<String> args)
    {
        String javaBin = STR."\{System.getProperty("java.home")}\{File.separator}bin\{File.separator}java";

        var processBuilder = new ProcessBuilder(
                javaBin,
                "--enable-preview",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens", "java.base/jdk.internal.util=ALL-UNNAMED",
                "-jar",
                jarPath,
                STR."\{recoverEnabled}"
        );
        processBuilder.command().addAll(args);

        processBuilder.directory(new File("../flightScheduler-customer"));

        processBuilder.inheritIO();
        return processBuilder;
    }

    public static void KillCurrentVmsProcess(String vmsIdentifier)
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
    public static ProcessBuilder VmsProcessBuilder(String vmsIdentifier, boolean recoverEnabled, List<String> args)
    {
        var jarPath = STR."../flightScheduler-\{vmsIdentifier}/target/flightScheduler-\{vmsIdentifier}-1.0-SNAPSHOT-jar-with-dependencies.jar";
        var processBuilder = CreateProcessBuilder(jarPath, recoverEnabled, args);

        return processBuilder;
    }

    public static void KillComponents(List<String> components)
    {
        for (var component : components)
        {
            VmsProcess.KillCurrentVmsProcess(component);
        }
    }
    public static void StartComponents(List<String> components) throws IOException
    {
        for (var component : components)
        {
            VmsProcess.VmsProcessBuilder(component, false, List.of()).start();
        }
    }
}
