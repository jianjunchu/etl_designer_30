package org.pentaho.di.trans.steps.resumesplitter;

import java.util.Date;

public class ResumeSegment {

    private String startDate;
    private String endDate;
    private int resumeType;
    private String content;
    private String orgnization;
    private String workPosition; //职务
    private String studyPosition; //学位

    public static final int  RESUME_TYPE_STUDY=1;
    public static final int RESUME_TYPE_WORK=2;

    public static final String  RESUME_TYPE_STUDY_DESC="学习";
    public static final String  RESUME_TYPE_WORK_DESC="工作";
    public static final String  RESUME_TYPE_NONE_DESC="无类型";


    public static String[] WORKING_POSITIONS = new String[]{"党委书记","党委副书记","纪检组组长","党组成员","院士","博士后","硕士后","学士后","党委书记","党委委员","省委书记","省委委员","市委书记","市委委员","区委书记","区委委员","县委书记","副主席","主席","县委委员","副部长","部长","副市长","市长","副厅长","厅长","副局长","局长","副研究员","助理研究员","研究员","副校长","校长","副所长","所长","副院长","院长","副秘书长","秘书长","秘书","副局长","局长","副处长","处长","副教授","教授","博士生导师","硕士生导师","教师","讲师","助教","副主任","主任","副科长","科长","副组长","组长","科员","工人","干事","技术员"};
    public static String[] STUDY_POSITIONS = new String[]{"博士","硕士","学士","本科"};

    public static String[] RESUME_TYPE_STUDY_WORDS_1 = new String[]{"考入","从师","学习","入学","攻读","毕业"};//通过简单匹配可以确认是学习类型的简历
    public static String[] RESUME_TYPE_STUDY_WORDS_2 = new String[]{"大学","学校","学院"};//除了简单匹配还需要配合其他条件，来确认是学习类型的简历

    public static String[] RESUME_TYPE_WORK_WORDS_1 = new String[]{"下乡","任教","认职","调动","借调","调入","入职","挂职","兼职","工作","支教","支边","参军","服役","出版社"};//通过简单匹配可以确认是工作类型的简历

    public String getStudyPosition() {
        return studyPosition;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public int getResumeType() {
        return resumeType;
    }

    public String getContent() {
        return content;
    }

    public String getOrgnization() {
        return orgnization;
    }

    public String getWorkPosition() {
        return workPosition;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public void setResumeType(int resumeType) {
        this.resumeType = resumeType;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public void setOrgnization(String orgnization) {
        this.orgnization = orgnization;
    }

    public void setWorkPosition(String position) {
        this.workPosition = position;
    }

    public void setStudyPosition(String studyPosition) {
        this.studyPosition = studyPosition;
    }

    public String getResumeTypeDesc() {
        switch(resumeType)
        {
            case RESUME_TYPE_STUDY:
                return RESUME_TYPE_STUDY_DESC;
            case RESUME_TYPE_WORK:
                return RESUME_TYPE_WORK_DESC;
        }
        return RESUME_TYPE_NONE_DESC;
    }
}
