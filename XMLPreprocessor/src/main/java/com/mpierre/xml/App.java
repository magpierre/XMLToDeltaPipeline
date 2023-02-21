package com.mpierre.xml;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;

public class App 
{
    public static void main( String[] args )
    {
       
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(false);
        factory.setValidating(false);
        factory.setXIncludeAware(false);

        try {
            SAXParser parser = factory.newSAXParser();
            XMLHandler handler = new XMLHandler();
            parser.parse(System.in, handler);
            
                
        }
        catch (SAXException e) {
            System.err.println(e.getMessage());
        }
        catch(ParserConfigurationException e) {
            System.err.println(e.getMessage());
        }
        catch(IOException e){
            System.err.println(e.getMessage());
        }
    }
}
