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

public final class LoggingHandlerBuilder {

    public static ILoggingHandler build(String identifier) {
        return build(identifier, new VmsSerdesProxyBuilder().build(), 16384, false);
    }

    public static ILoggingHandler build(String identifier, IVmsSerdesProxy serdesProxy, int bufferSize, boolean truncate)
    {
        String fileName = identifier + "_output.llog";
        String userHome = ConfigUtils.getUserHome();
        String basePath = userHome + "/vms";
        File theDir = new File(basePath);
        assert theDir.exists() || theDir.mkdirs();
        String filePath = basePath + "/" + fileName;
        Path path = Paths.get(filePath);

        FileChannel fileChannel;

        System.out.println(STR."Log with write_buffers_size \{bufferSize}\{truncate ? " truncating" : ""} at file filePath: \{filePath}");


        StandardOpenOption[] options;
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

        ILoggingHandler handler;
        try {
            fileChannel = FileChannel.open(path, options);
            handler = new ThesisLoggingHandler(fileChannel, serdesProxy, bufferSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return handler;
    }

}
