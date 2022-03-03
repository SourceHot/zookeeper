package org.apache.jute;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DemoRecord implements Record {
    private String name;
    private int age;

    public DemoRecord() {
    }

    public DemoRecord(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public static void main(String[] args) throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputArchive boa = BinaryOutputArchive.getArchive(baos);
        DemoRecord zhangsan = new DemoRecord("zhangsan", 10);
        zhangsan.serialize(boa, "data1");

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        InputArchive bia = BinaryInputArchive.getArchive(bais);
        DemoRecord demoRecord = new DemoRecord();
        demoRecord.deserialize(bia, "data1");



        baos.close();
        bais.close();
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        archive.writeInt(age, "age2i2222");
        archive.writeString(name, "name");
        archive.endRecord(this, tag);

    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        this.age = archive.readInt("age321");
        this.name = archive.readString("name");
        archive.endRecord(tag);

    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

}
