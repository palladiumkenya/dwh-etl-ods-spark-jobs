package org.kenyahmis.core;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class FileUtils<T> {

    public  String loadTextFromFile(Class<T> t, String fileName) throws IOException {
        String text = null;
        InputStream inputStream = t.getClassLoader().getResourceAsStream(fileName);
        if (inputStream != null) {
            text = IOUtils.toString(inputStream, Charset.defaultCharset());
        }
        return text;
    }
}
