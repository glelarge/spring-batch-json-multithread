package org.springframework.batch;

/*
CREATE TABLE mytable (
    CODE INTEGER NOT NULL ,
    REF  VARCHAR(13) NOT NULL ,
    TYPE INTEGER NOT NULL,
    NATURE INTEGER,
    ETAT INTEGER NOT NULL,
    REF2 VARCHAR(13)
);
 */

public class MyData implements java.io.Serializable {
    private Integer code;
    private String ref;
    private Integer type;
    private Integer nature;
    private Integer etat;
    private String ref2;

    public MyData(Integer code, String ref, Integer type, Integer nature, Integer etat, String ref2) {
        this.code = code;
        this.ref = ref;
        this.type = type;
        this.nature = nature;
        this.etat = etat;
        this.ref2 = ref2;
    }

    // Getters and setters
    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getNature() {
        return nature;
    }

    public void setNature(Integer nature) {
        this.nature = nature;
    }

    public Integer getEtat() {
        return etat;
    }

    public void setEtat(Integer etat) {
        this.etat = etat;
    }

    public String getRef2() {
        return ref2;
    }

    public void setRef2(String ref2) {
        this.ref2 = ref2;
    }
}
