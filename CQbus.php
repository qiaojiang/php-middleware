<?php
require_once '/home/q/php/qbus/qbus.php';

/**
 * 生产者类
 */
class CQbusProducer 
{ 
    //集群
    private $cluster;
    //topic
    private $topic;
    //日志文件
    private $logFile = '/tmp/qbus.log';
    //配置文件,为空时是默认配置
    private $configFile = '';
    //生产者
    private $producer = null;

    //错误重试次数
    const RETRY_NUM = 3;
    
    function __construct($cluster, $topic, $logFile = '',$configFile = '')
    {
        $this->cluster = $cluster;
        $this->topic = $topic;
        if(!empty($logFile)){
            $this->logFile = $logFile;
        }
        if(!empty($configFile)){
            $this->configFile = $configFile;
        }
        //初始化QbusProducer
        $this->initProducer();
    }
    
    /**
     * 初始化Producer
     */
    private function initProducer()
    {
        try {
            $this->producer = new QbusProducer;
            $flag = $this->producer->init($this->cluster, $this->logFile, $this->configFile, $this->topic);
            if(!$flag){
                throw new Exception("init qbus producer failed");
            }
            return $flag;
        }catch (Exception $e){
            fprintf(STDERR, "[%s] %s\r\n", date('Y-m-d H:i:s'), $e->getMessage());
            return false;
        }
    }
    
    /**
     * 生产数据
     * @param string $data 数据
     * @param string $key 若key为空字串，则写入qbus的数据在各partition间均匀分布;否则，相同key的数据分布在相同的partition上
     * @return boolean
     */
    public function produce($data,$key = '')
    {
        $i = 0;
        $flag = false;
        while ($i++ < self::RETRY_NUM){
            $flag = $this->producer->produce($data, strlen($data), $key);
            if ($flag) {
                break;
            }
            usleep(10);
        }
        return $flag;
    }
    
    
    public function __destruct()
    {
        $this->producer->uninit();
    }

}     

/**
 * 消费者类
 */
class CQbusConsumer
{
    //集群
    private $cluster;
    //topic
    private $topic;
    //group
    private $group;
    //日志文件
    private $logFile = '/tmp/qbus.log';
    //配置文件,为空时是默认配置
    private $configFile = '';
    //消费者
    private $consumer = null;



    function __construct($cluster, $topic, $group, $logFile = '', $configFile = '')
    {
        $this->cluster = $cluster;
        $this->topic = $topic;
        $this->group = $group;
        if(!empty($logFile)){
            $this->logFile = $logFile;
        }
        if(!empty($configFile)){
            $this->configFile = $configFile;
        }
        //初始化QbusConsumer
        $this->initConsumer();
    }
    
    /**
     * 初始化consumer
     */
    private function initConsumer()
    {
        try {
            $this->consumer = new QbusConsumer;
            $flag = $this->consumer->init($this->cluster, $this->logFile, $this->configFile);
            if(!$flag){
                throw new Exception(__CLASS__." failed init");
            }
            $flag = $this->consumer->subscribeOne($this->group, $this->topic);
            if(!$flag){
                throw new Exception(__CLASS__." failed subscribe");
            }
            $this->consumer->start();
            return $flag;
        }catch (Exception $e){
            fprintf(STDERR, "[%s] %s\r\n", date('Y-m-d H:i:s'), $e->getMessage());
            return false;
        }
    }

    /**
     * 消费数据
     * 该函数是阻塞的，没数据时会一直等待，直到获取1条数据。
     */
    public function consume()
    {
        static $msgInfo;
        if(is_null($msgInfo)) $msgInfo = new QbusMsgContentInfo;
        $data = array();
        if ($this->consumer->consume($msgInfo)){
            $data = array('topic' => $msgInfo->topic,'msg' => $msgInfo->msg);
        }
        return $data;
    }


    public function __destruct()
    {
        $this->consumer->stop();
    }

}
