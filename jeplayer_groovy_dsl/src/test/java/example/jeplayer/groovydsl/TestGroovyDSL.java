package example.jeplayer.groovydsl;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import java.io.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author jmarranz
 */
public class TestGroovyDSL
{
    public TestGroovyDSL()
    {
    }
    
    // Here the test setup stuff...
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }
        
    @Test
    public void groovyDSLExample() throws Exception
    {          
        String basePath = "src/test/groovy";
        ClassLoader parent = getClass().getClassLoader();
        GroovyClassLoader loader = new GroovyClassLoader(parent);
        loader.addClasspath(basePath);
        Class groovyClass = loader.parseClass(new File(basePath + "/example/jeplayer/groovydsl/GroovyDSLExample.groovy"));

        // let's call some method on an instance
        GroovyObject groovyObject = (GroovyObject) groovyClass.newInstance();
        Object[] args = {};
        groovyObject.invokeMethod("run", args);
    }
    

}

