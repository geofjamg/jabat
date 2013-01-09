package fr.jamgotchian.jabat.runtime.artifact;

public interface ArtifactFactory {

    public void initialize();

    public Object create(String name);

    public void destroy(Object instance);

}
