package org.jorgerojasdev;

import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.PluginExecution;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.twdata.maven.mojoexecutor.MojoExecutor.*;


@Mojo(name = "schema", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class AvroDependenciesMojoImports extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true)
    private MavenProject project;

    @Parameter(defaultValue = "${session}", readonly = true)
    private MavenSession mavenSession;

    @Parameter(required = true)
    private String avroVersion;

    @Parameter(required = true)
    private Map<String, Object> avroConfig;

    @Component
    private BuildPluginManager pluginManager;

    private final String defaultAvroFolderPath = "src/main/resources/avro";

    private final String divider = "------------------------------------------------------------------------";

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info(divider);
        getLog().info("Resolving imports from avro...");

        List<Element> elementList = mapAvroConfigToElementList();

        List<Element> importList = resolveImportAvro();
        getLog().info(String.format("Found necessary imports: %s", importList.size()));
        importList.forEach(importElement -> getLog().info(importElement.toDom().getValue()));
        elementList.add(element("imports", importList.toArray(new Element[importList.size()])));

        getLog().info(divider);
        getLog().info("Launching avro-maven-plugin...");
        executeAvroPlugin(elementList.toArray(new Element[elementList.size()]));
    }

    private List<Element> mapAvroConfigToElementList() {
        return avroConfig.entrySet().stream().flatMap((entry) -> {
            String key = entry.getKey();
            Object value = entry.getValue();
            if ("imports".equals(key)) {
                return Stream.empty();
            }
            if (String.class.equals(value.getClass())) {
                return Stream.of(element(key, value.toString()));
            }
            return Stream.empty();
        }).collect(Collectors.toList());
    }

    private List<Element> resolveImportAvro() {
        String avroFolderPath = defaultAvroFolderPath;
        Object configSourceDirectory = avroConfig.get("sourceDirectory");
        if (configSourceDirectory != null) {
            avroFolderPath = String.valueOf(configSourceDirectory);
        }
        List<File> avroFiles = Arrays.asList(getFilesFromAvroFolder(avroFolderPath));
        Map<String, ClassifiedFile> classifiedAvroFilesMap = mapFilesToClassifiedFilesMap(avroFiles);
        List<ClassifiedFile> sortedClassifiedFiles = resolveAndSortDependenciesBetweenAvros(classifiedAvroFilesMap).stream().distinct().collect(Collectors.toList());
        return sortedClassifiedFiles.stream().filter(classifiedFile -> classifiedFile.getDependsIn().size() > 0).map(classifiedFile -> element("import", classifiedFile.getFile().getAbsolutePath())).collect(Collectors.toList());
    }

    private void executeAvroPlugin(Element... imports) throws MojoFailureException {
        try {
            executeMojo(
                    plugin(
                            groupId("org.apache.avro"),
                            artifactId("avro-maven-plugin"),
                            version(avroVersion)
                    ),
                    goal("schema"),
                    configuration(
                            imports
                    ),
                    executionEnvironment(
                            project,
                            mavenSession,
                            pluginManager
                    )
            );
        } catch (Exception e) {
            getLog().error(e.getCause().getMessage());
            throw new MojoFailureException(e.getCause().getMessage());
        }
    }

    private Xpp3Dom getConfigurationFromAvroMavenPlugin(List<PluginExecution> pluginExecutions) throws MojoFailureException {
        for (PluginExecution execution : pluginExecutions) {
            return (Xpp3Dom) execution.getConfiguration();
        }
        throw new MojoFailureException("Not founded Configuration in avro-maven-plugin execution");
    }

    private Plugin getAvroMavenPlugin(List<Plugin> plugins) throws MojoFailureException {
        for (Plugin plugin : project.getBuildPlugins()) {
            if ("avro-maven-plugin".equals(plugin.getArtifactId())) {
                return plugin;
            }
        }
        throw new MojoFailureException("avro-depencencies requires avro-maven-plugin");
    }

    private File[] getFilesFromAvroFolder(String avroFolderPath) {
        File folder = new File(avroFolderPath);
        return folder.listFiles((dir, name) -> name.endsWith(".avsc"));
    }

    private Map<String, ClassifiedFile> mapFilesToClassifiedFilesMap(List<File> files) {
        Map<String, ClassifiedFile> result = new HashMap<>();
        for (File file : files) {
            result.put(file.getName().replace(".avsc", ""), new ClassifiedFile(file));
        }
        return result;
    }

    private List<ClassifiedFile> resolveAndSortDependenciesBetweenAvros(Map<String, ClassifiedFile> classifiedFileMap) {
        List<String> avroFileNames = new ArrayList<>(classifiedFileMap.keySet());
        classifiedFileMap.forEach((currentAvroFileName, classifiedFile) -> {
            try (FileReader fileReader = new FileReader(classifiedFile.getFile());
                 BufferedReader bufferedReader = new BufferedReader(fileReader)) {
                String currentLine;
                List<String> coincidentAvrosInFile = new ArrayList<>();
                Stream<String> coincidentAvrosStream = coincidentAvrosInFile.stream();
                while ((currentLine = bufferedReader.readLine()) != null) {
                    coincidentAvrosStream = Stream.concat(
                            coincidentAvrosStream,
                            coincidentAvrosInText(currentLine, avroFileNames).stream());
                }
                coincidentAvrosInFile = coincidentAvrosStream.distinct().collect(Collectors.toList());
                addCoincidentsAtClassifiedAvroFileMap(currentAvroFileName, classifiedFileMap, coincidentAvrosInFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return sortAvroClassifiedFiles(classifiedFileMap);
    }

    private List<String> coincidentAvrosInText(String text, List<String> avroFileNames) {
        List<String> coincidentAvros = new ArrayList<>();
        avroFileNames.forEach(avroFileName -> {
            if (text.contains("\"type\"") && text.contains(avroFileName)) {
                coincidentAvros.add(avroFileName);
            }
        });

        return coincidentAvros;
    }

    private void addCoincidentsAtClassifiedAvroFileMap(String currentAvroFilename, Map<String, ClassifiedFile> classifiedFileMap, List<String> coincidences) {
        coincidences.forEach(coincidence -> {
            if (!classifiedFileMap.get(coincidence).getDependsIn().contains(currentAvroFilename)) {
                classifiedFileMap.get(coincidence).getDependsIn().add(currentAvroFilename);
            }
        });
    }

    private List<ClassifiedFile> sortAvroClassifiedFiles(Map<String, ClassifiedFile> classifiedFileMap) {
        List<ClassifiedFile> classifiedFiles = classifiedFileMap.values().stream().collect(Collectors.toList());
        getLog().info("Sorting avros:");
        classifiedFiles.forEach(classifiedFile -> getLog().info(classifiedFile.toString()));
        classifiedFiles.sort((a, b) -> {
            int aDepNum = stepDependenciesNumber(classifiedFileMap, a);
            int bDepNum = stepDependenciesNumber(classifiedFileMap, b);
            if (aDepNum > bDepNum) {
                return -1;
            }
            if (aDepNum == bDepNum) {
                return 0;
            }
            return 1;
        });
        return classifiedFiles;
    }

    private int stepDependenciesNumber(Map<String, ClassifiedFile> classifiedFileMap, ClassifiedFile classifiedFile) {
        int counter = 0;

        if (classifiedFile.getDependsIn().isEmpty()) {
            return counter;
        }

        counter++;

        List<Integer> subCounters = new ArrayList<>();

        classifiedFile.getDependsIn().forEach(avroDependantName -> {
            subCounters.add(stepDependenciesNumber(classifiedFileMap, classifiedFileMap.get(avroDependantName)));
        });

        return counter + subCounters.stream().max(Integer::compareTo).orElse(0);
    }

}