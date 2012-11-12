package fr.jamgotchian.jabat.cdi;

import fr.jamgotchian.jabat.spi.ArtifactFactory;
import java.util.Set;
import javax.enterprise.inject.spi.Bean;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatCdiArtifactFactory implements ArtifactFactory {

    @Override
    public void initialize() throws Exception {
        if (JabatCdiExtension.BEAN_MANAGER == null) {
            throw new Exception("CDI container not initialized");
        }
    }

    @Override
    public Object create(String ref) throws Exception {
        Set<Bean<?>> beans = JabatCdiExtension.BEAN_MANAGER.getBeans(ref);
        if (beans.isEmpty()) {
            throw new Exception("Batch artifact " + ref + " not found");
        }
        return beans.iterator().next().create(null);
    }

    @Override
    public void destroy(Object instance) throws Exception {
        // TODO
    }

}
