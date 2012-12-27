package fr.jamgotchian.jabat.artifact;

public interface ArtifactFactory {

    /**
     * The initialize method is invoked once during the initialization of the
     * batch runtime.
     *
     * @throws Exception if artifact factory cannot be loaded. The batch runtime
     * responds by issuing an error message and disabling itself.
     */
    public void initialize() throws Exception;

    /**
     * The create method creates an instance corresponding to a ref value from a
     * Job XML.
     *
     * @param ref value from Job XML
     * @return instance corresponding to ref value
     * @throws Exception if instance cannot be created.
     */
    public Object create(String ref) throws Exception;

    /**
     * The destroy method destroys an instance created by this factory.
     *
     * @param instance to destroy
     * @throws Exception if instance cannot be destroyed.
     */
    public void destroy(Object instance) throws Exception;
}
