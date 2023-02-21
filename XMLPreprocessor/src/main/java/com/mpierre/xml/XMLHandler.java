package com.mpierre.xml;
import java.util.Stack;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

class XMLHandler extends DefaultHandler {
    private String content;
    private Stack<String> stk = new Stack<String>();
    public XMLHandler() {
    }
    /*
     * (non-Javadoc)
     * 
     * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
     */
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        content += new String(ch, start, length).replace(",","¶").replace("\n"," ");
    }
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        String tmp = stk.pop();
        String tmpStr= new String();
        int attr_start;

       
        if (tmp.trim().length() > 0) {
            String initalString = stk.toString();
            String s = initalString.substring(1, initalString.length() - 1);
           
            attr_start = s.indexOf('[');
            if (attr_start < 0) {
                tmpStr = s.replace(',', '¶').replace(" ", "");
            } else {
                while (s.length() > 0 ) {
                    int attr_end = s.indexOf(']');
                    if (attr_end <= 0) {
                        attr_end = s.length();
                    }
                    String remainder = new String();
                    String attribs = new String();
                    if( attr_end > 0 && attr_end > attr_start && attr_end < s.length()) {
                        remainder = s.substring(attr_end+1);
                        attribs = s.substring(attr_start, attr_end+1);
                        s.replace(attribs,"");
                    }
                    String node = new String();
                    if(attr_start > 0) {
                        node = s.substring(0,attr_start);
                    } else {
                        node = s;
                    }
                   
                    String cleaned_node_tree = new String();
                    int test_val = node.indexOf('=');
                    if( test_val < 0) {
                         cleaned_node_tree =  node.replace(',', '¶').replace(" ", "");
                    } else {
                        cleaned_node_tree = node;
                    }
                    tmpStr += cleaned_node_tree + attribs;
                    s = remainder;
                    attr_start = s.indexOf('[');
                    remainder = new String();

                    
                } 
            }
            s = new String();
            if(content.trim().length() > 0) {
                System.out.println(tmpStr + "," + tmp + "," + content);
                content = new String();
            }
        }
    }
    /*
     * (non-Javadoc)
     * 
     * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String,
     * java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);
        String attribString = new String();
        content = new String();
        for (int i = 0; i < attributes.getLength(); i++) {
            if (i < 1) {
                attribString = attributes.getQName(i).trim() + "=" + attributes.getValue(i).trim().replace(",", "¡");
            } else {
                attribString += "¡" + attributes.getQName(i).trim() + "=" + attributes.getValue(i).trim().replace(",","!");
            }
        }
        if (attribString.length() > 0) {
            stk.push(qName.trim() + "[" + attribString.trim() + "]");
        } else {
            stk.push(qName.trim());
        }
    }
}