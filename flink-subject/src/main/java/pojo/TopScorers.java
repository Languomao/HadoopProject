package pojo;

/**
 * Created by Languomao on 2019/7/31.
 */
/**
 * 源数据的映射类
 */
public  class TopScorers {
    /**
     * 排名，球员，球队，出场，进球，射正，任意球，犯规，黄牌，红牌
     */
    public Integer rank;
    public String player;
    public String club;
    public Integer chuchang;
    public Integer jinqiu;
    public Integer zhugong;
    public Integer shezheng;
    public Integer renyiqiu;
    public Integer fangui;
    public Integer huangpai;
    public Integer hongpai;

    public TopScorers() {
        super();
    }

    public TopScorers(Integer rank, String player, String club, Integer chuchang, Integer jinqiu, Integer zhugong, Integer shezheng, Integer renyiqiu, Integer fangui, Integer huangpai, Integer hongpai) {
        this.rank = rank;
        this.player = player;
        this.club = club;
        this.chuchang = chuchang;
        this.jinqiu = jinqiu;
        this.zhugong = zhugong;
        this.shezheng = shezheng;
        this.renyiqiu = renyiqiu;
        this.fangui = fangui;
        this.huangpai = huangpai;
        this.hongpai = hongpai;
    }
}

