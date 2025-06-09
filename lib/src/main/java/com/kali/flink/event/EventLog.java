package com.kali.flink.event;

public class EventLog {
    private String time;
    private String entryid;
    private String event;
    private String project;
    private String type;
    private String trackSign;
    private String operationNname;
    private String locationType;
    private String isLogin;
    private String platformName;
    private String productCode;
    private String positionFullName;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getEntryid() {
        return entryid;
    }

    public void setEntryid(String entryid) {
        this.entryid = entryid;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTrackSign() {
        return trackSign;
    }

    public void setTrackSign(String trackSign) {
        this.trackSign = trackSign;
    }

    public String getOperationNname() {
        return operationNname;
    }

    public void setOperationNname(String operationNname) {
        this.operationNname = operationNname;
    }

    public String getLocationType() {
        return locationType;
    }

    public void setLocationType(String locationType) {
        this.locationType = locationType;
    }

    public String getIsLogin() {
        return isLogin;
    }

    public void setIsLogin(String isLogin) {
        this.isLogin = isLogin;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getPositionFullName() {
        return positionFullName;
    }

    public void setPositionFullName(String positionFullName) {
        this.positionFullName = positionFullName;
    }

    @Override
    public String toString() {
        return "EventLog{" +
                "time='" + time + '\'' +
                ", entryid='" + entryid + '\'' +
                ", event='" + event + '\'' +
                ", project='" + project + '\'' +
                ", type='" + type + '\'' +
                ", trackSign='" + trackSign + '\'' +
                ", operationNname='" + operationNname + '\'' +
                ", locationType='" + locationType + '\'' +
                ", isLogin='" + isLogin + '\'' +
                ", platformName='" + platformName + '\'' +
                ", productCode='" + productCode + '\'' +
                ", positionFullName='" + positionFullName + '\'' +
                '}';
    }
}