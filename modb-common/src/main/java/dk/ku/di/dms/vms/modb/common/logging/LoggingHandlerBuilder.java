package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Properties;

public final class LoggingHandlerBuilder {

    public static ILoggingHandler build(String identifier) {
        return build(identifier, new VmsSerdesProxyBuilder().build(), 1000, false);
    }

    public static ILoggingHandler build(String identifier, IVmsSerdesProxy serdesProxy, int bufferSize, boolean truncate) {
//        String fileName = identifier + "_" + new Date().getTime() +".llog";
        Properties props = ConfigUtils.loadProperties();

        String fileName = identifier + ".llog";

        StandardOpenOption[] options = buildFileOpenOptions(truncate);
        var file = buildFile(fileName);

        String loggingType = props.getProperty("logging_type");
        ILoggingHandler handler;
        try {
            FileChannel fileChannel = FileChannel.open(Path.of(file.toURI()), options);
            handler = new ThesisLoggingHandlerV1(fileChannel, serdesProxy, bufferSize);

        }
        catch (NoClassDefFoundError | Exception e) {
            return null;
        }
        return handler;
    }


    private static StandardOpenOption[] buildFileOpenOptions(boolean truncate) {
        StandardOpenOption[] options;
//        System.out.println(STR."buildFileOpenOptions truncating=\{truncate}");
        if(truncate){
            options = new StandardOpenOption[]{
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.SPARSE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            };
        } else {
            options = new StandardOpenOption[]{
                    StandardOpenOption.CREATE,
                    StandardOpenOption.SPARSE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            };
        }
        return options;
    }

    private static File buildFile(String fileName) {
        String currentDir = System.getProperty("user.dir");
        String basePath = currentDir + "/logs";
        String filePath = basePath + "/" + fileName;
        File file = new File(filePath);
        // System.out.println(STR."filePath: \{filePath}");
        return file;
    }

}
