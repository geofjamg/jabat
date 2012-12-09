package fr.jamgotchian.jabat.artifact.impl;

import fr.jamgotchian.jabat.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.util.JabatException;
import java.util.Set;
import javax.enterprise.inject.spi.Bean;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class CdiArtifactFactory implements ArtifactFactory {

    private final BatchXmlArtifactFactory batchXmlArtifactFactory
            = new BatchXmlArtifactFactory();

    @Override
    public void initialize() throws Exception {
        if (JabatCdiExtension.BEAN_MANAGER == null) {
            throw new JabatException("CDI container not initialized");
        }
        batchXmlArtifactFactory.initialize();
    }

    @Override
    public Object create(String ref) throws Exception {
        Object instance;
        Set<Bean<?>> beans = JabatCdiExtension.BEAN_MANAGER.getBeans(ref);
        if (beans.isEmpty()) {
            // there is no artifact annotated by @Named, search into the batch.xml
            instance = batchXmlArtifactFactory.create(ref);
        } else {
            // there is an artifact annotated by @Named
            instance = beans.iterator().next().create(null);
        }
        return instance;
    }

    @Override
    public void destroy(Object instance) throws Exception {
        // TODO
    }

}