package rddShare.core;

import junit.framework.TestCase;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.TreeSet;

/**
 * Created by hcq on 16-5-9.
 */
public class RDDShare$Test extends TestCase {

    public void testMain() throws Exception {

    }

    public void testGetBasePath() throws Exception {
    }

    public void testConf() throws Exception {
        System.out.println(RDDShare.conf().toString());
        System.out.println(RDDShare.tranformtion_priority());
    }

    public void testPath() throws Exception {
        System.out.println(RDDShare.sparkCorePath());
    }

    public void testGetRepository() throws Exception {

    }

    public void setUp() throws Exception {
        TreeSet<String> rep = new TreeSet<>();
        String cache = "hello";
        rep.add(cache);
        FileOutputStream file = new FileOutputStream(RDDShare.resourcesPath()+"string");
        ObjectOutputStream out = new ObjectOutputStream(file);
        out.writeObject(rep);
        out.close();

        FileInputStream fileread = new FileInputStream(RDDShare.resourcesPath()+"string");
        ObjectInputStream in = new ObjectInputStream(fileread);
        TreeSet<String> s = (TreeSet<String>) in.readObject();
        System.out.print(s.first());

        super.setUp();

    }

    public void testGetRepository1() throws Exception {
        RDDShare.getRepository();
    }
}