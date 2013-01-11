package fr.jamgotchian.jabat.runtime.artifact;

public interface ArtifactFactory {

    public Object create(String name);

    public void destroy(Object instance);

}
