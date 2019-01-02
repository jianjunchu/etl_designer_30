package org.pentaho.di.core.util.function.datastage;


public class TestClass {
    private static final long serialVersionUID = 1L;
    public static void main(String[] args)  throws Exception {
        Dcount dts=new Dcount();
        Object[] obj={"chocolate drops, chocolate ice cream, chocolate bars","choc"};
        System.out.print(dts.executeInner(obj));
    }

}