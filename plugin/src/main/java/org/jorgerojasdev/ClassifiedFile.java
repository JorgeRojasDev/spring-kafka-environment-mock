package org.jorgerojasdev;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ClassifiedFile {

    private String filename;

    private File file;

    private List<String> dependsIn = new ArrayList<>();

    public ClassifiedFile(File file) {
        this.filename = file.getName().replace(".avsc", "");
        this.file = file;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public List<String> getDependsIn() {
        return dependsIn;
    }

    public void setDependsIn(List<String> dependsIn) {
        this.dependsIn = dependsIn;
    }

    @Override
    public String toString() {
        return "ClassifiedFile{" +
                "filename='" + filename + '\'' +
                ", file=" + file +
                ", dependsIn=" + dependsIn +
                '}';
    }
}
