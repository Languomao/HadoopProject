package pojo;

/**
 * Created by Languomao on 2019/8/1.
 */
public class VideoObject {

    public String Interface;
    public String IMSI;
    public String MSISDN;
    public String IMEISV;
    public String APN;
    public String DestinationIP;
    public String DestinationPort;
    public String SourceIP;
    public String SourcePort;
    public String SGWIP;
    public String MMEIP;
    public String PGWIP;
    public String ECGI;
    public String TAI;
    public String VisitedPLMNId;
    public String RATType;
    public String ProtocolID;
    public String ServiceType;
    public String StartTime;
    public String EndTime;
    public String Duration;
    public String InputOctets;
    public String OutputOctets;
    public String InputPacket;
    public String OutputPacket;
    public String PDNConnectionId;
    public String BearerID;
    public String BearerQoS;
    public String RecordCloseCause;
    public String ENBIP;
    public String SGWPort;
    public String eNBPort;
    public String eNBGTP_TEID;
    public String SGWGTP_TEID;
    public String PGWPort;
    public String MMEUES1APID;
    public String eNBUES1APID;
    public String MMEGroupID;
    public String MMECode;
    public String eNBID;
    public String Home_Province;
    public String UserIP;
    public String UserPort;
    public String L4protocol;
    public String AppServerIP;
    public String AppServerPort;
    public String ULTCPReorderingPacket;
    public String DLTCPReorderingPacket;
    public String ULTCPRetransPacket;
    public String DLTCPRetransPacket;
    public String TCPSetupResponseDelay;
    public String TCPSetupACKDelay;
    public String ULIPFragPacks;
    public String DLIPFragPacks;
    public String Delay_Setup_FirstTransaction;
    public String Delay_FirstTransaction_FirstResPackt;
    public String WindowSize;
    public String MSSSize;
    public String TCPSynNumber;
    public String TCPConnectState;
    public String SessionStopFlag;
    public String UEAMBRUL;
    public String UEAMBRDL;
    public String BearerQCI;
    public String VideoPlaySuccess;
    public String VideoPlayWaitTime;
    public String VideoPlayHaltCount;
    public String VideoRecoverTime;
    public String FirstDataPkgTime;
    public String LastDataPkgTime;
    public String VideoApp;
    public String VideoSize;
    public String VideoThroughput;
    public String VideoType;
    public String VideoBitRate;
    public String VideoCacheThroughput;
    public String DestinationURL;
    public String Host;
    public String CachedVideoSize;
    public String CachedVideoDuration;

    public VideoObject() {
        super();
    }

    public VideoObject(String anInterface, String IMSI, String MSISDN, String IMEISV, String APN, String destinationIP, String destinationPort, String sourceIP, String sourcePort, String SGWIP, String MMEIP, String PGWIP, String ECGI, String TAI, String visitedPLMNId, String RATType, String protocolID, String serviceType, String startTime, String endTime, String duration, String inputOctets, String outputOctets, String inputPacket, String outputPacket, String PDNConnectionId, String bearerID, String bearerQoS, String recordCloseCause, String ENBIP, String SGWPort, String eNBPort, String eNBGTP_TEID, String SGWGTP_TEID, String PGWPort, String MMEUES1APID, String eNBUES1APID, String MMEGroupID, String MMECode, String eNBID, String home_Province, String userIP, String userPort, String l4protocol, String appServerIP, String appServerPort, String ULTCPReorderingPacket, String DLTCPReorderingPacket, String ULTCPRetransPacket, String DLTCPRetransPacket, String TCPSetupResponseDelay, String TCPSetupACKDelay, String ULIPFragPacks, String DLIPFragPacks, String delay_Setup_FirstTransaction, String delay_FirstTransaction_FirstResPackt, String windowSize, String MSSSize, String TCPSynNumber, String TCPConnectState, String sessionStopFlag, String UEAMBRUL, String UEAMBRDL, String bearerQCI, String videoPlaySuccess, String videoPlayWaitTime, String videoPlayHaltCount, String videoRecoverTime, String firstDataPkgTime, String lastDataPkgTime, String videoApp, String videoSize, String videoThroughput, String videoType, String videoBitRate, String videoCacheThroughput, String destinationURL, String host, String cachedVideoSize, String cachedVideoDuration) {
        this.Interface = anInterface;
        this.IMSI = IMSI;
        this.MSISDN = MSISDN;
        this.IMEISV = IMEISV;
        this.APN = APN;
        this.DestinationIP = destinationIP;
        this.DestinationPort = destinationPort;
        this.SourceIP = sourceIP;
        this.SourcePort = sourcePort;
        this.SGWIP = SGWIP;
        this.MMEIP = MMEIP;
        this.PGWIP = PGWIP;
        this.ECGI = ECGI;
        this.TAI = TAI;
        this.VisitedPLMNId = visitedPLMNId;
        this.RATType = RATType;
        this.ProtocolID = protocolID;
        this.ServiceType = serviceType;
        this.StartTime = startTime;
        this.EndTime = endTime;
        this.Duration = duration;
        this.InputOctets = inputOctets;
        this.OutputOctets = outputOctets;
        this.InputPacket = inputPacket;
        this.OutputPacket = outputPacket;
        this.PDNConnectionId = PDNConnectionId;
        this.BearerID = bearerID;
        this.BearerQoS = bearerQoS;
        this.RecordCloseCause = recordCloseCause;
        this.ENBIP = ENBIP;
        this.SGWPort = SGWPort;
        this.eNBPort = eNBPort;
        this.eNBGTP_TEID = eNBGTP_TEID;
        this.SGWGTP_TEID = SGWGTP_TEID;
        this.PGWPort = PGWPort;
        this.MMEUES1APID = MMEUES1APID;
        this.eNBUES1APID = eNBUES1APID;
        this.MMEGroupID = MMEGroupID;
        this.MMECode = MMECode;
        this.eNBID = eNBID;
        this.Home_Province = home_Province;
        this.UserIP = userIP;
        this.UserPort = userPort;
        this.L4protocol = l4protocol;
        this.AppServerIP = appServerIP;
        this.AppServerPort = appServerPort;
        this.ULTCPReorderingPacket = ULTCPReorderingPacket;
        this.DLTCPReorderingPacket = DLTCPReorderingPacket;
        this.ULTCPRetransPacket = ULTCPRetransPacket;
        this.DLTCPRetransPacket = DLTCPRetransPacket;
        this.TCPSetupResponseDelay = TCPSetupResponseDelay;
        this.TCPSetupACKDelay = TCPSetupACKDelay;
        this.ULIPFragPacks = ULIPFragPacks;
        this.DLIPFragPacks = DLIPFragPacks;
        this.Delay_Setup_FirstTransaction = delay_Setup_FirstTransaction;
        this.Delay_FirstTransaction_FirstResPackt = delay_FirstTransaction_FirstResPackt;
        this.WindowSize = windowSize;
        this.MSSSize = MSSSize;
        this.TCPSynNumber = TCPSynNumber;
        this.TCPConnectState = TCPConnectState;
        this.SessionStopFlag = sessionStopFlag;
        this.UEAMBRUL = UEAMBRUL;
        this.UEAMBRDL = UEAMBRDL;
        this.BearerQCI = bearerQCI;
        this.VideoPlaySuccess = videoPlaySuccess;
        this.VideoPlayWaitTime = videoPlayWaitTime;
        this.VideoPlayHaltCount = videoPlayHaltCount;
        this.VideoRecoverTime = videoRecoverTime;
        this.FirstDataPkgTime = firstDataPkgTime;
        this.LastDataPkgTime = lastDataPkgTime;
        this.VideoApp = videoApp;
        this.VideoSize = videoSize;
        this.VideoThroughput = videoThroughput;
        this.VideoType = videoType;
        this.VideoBitRate = videoBitRate;
        this.VideoCacheThroughput = videoCacheThroughput;
        this.DestinationURL = destinationURL;
        this.Host = host;
        this.CachedVideoSize = cachedVideoSize;
        this.CachedVideoDuration = cachedVideoDuration;
    }
}
