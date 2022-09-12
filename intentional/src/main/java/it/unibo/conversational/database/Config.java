package it.unibo.conversational.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.List;

public final class Config {
    private static final Config c;

    static {
        // Get credentials from resources/config.example.yml.
        try {
            final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            c = mapper.readValue(Config.class.getClassLoader().getResource("config.example.yml"), Config.class);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("config.example.yml not found.");
        }
    }

    private Config() {
    }

    private static String webapp;
    public static String getWebapp() {
        return webapp;
    }
    public void setWebapp(final String webapp) {
        Config.webapp = webapp;
    }

    private List<Cube> cubes;
    private static String python;

    public static String getPython() {
        return python;
    }

    public void setPython(final String python) {
        Config.python = python;
    }

    public static Cube getCube(final String cube) {
        return getCubes().stream().filter(c -> c.getFactTable().toLowerCase().equals(cube.toLowerCase()) || c.getSynonyms().stream().anyMatch(s -> s.toLowerCase().equals(cube.toLowerCase()))).findFirst().get();
    }

    public static List<Cube> getCubes() {
        return c.cubes;
    }

    public void setCubes(final List<Cube> cubes) {
        this.cubes = cubes;
    }
}