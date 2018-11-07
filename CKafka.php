<?php
/**
 * Kafka 操作类
 */

namespace app\common\lib;

use yii\base\InvalidConfigException; 

class CKafka 
{
    //kafka brokers，多个用逗号隔开
    private $brokers = '';
    //生产者
    private $producer = null;
    //消费者
    private $consumer = null;
    
    public function __construct($brokers)
    {
        if (empty($brokers)) {
            throw new InvalidConfigException("brokers need");
        }
        $this->brokers = $brokers;
    }
    
    /**
     * 实例化生产者
     */
    private function _initProducer()
    {
        $rk = new \RdKafka\Producer();
        if (empty($rk)) {
            throw new InvalidConfigException("producer error");
        }
        $rk->setLogLevel(LOG_DEBUG);
        if (!$rk->addBrokers($this->brokers)) {
            throw new InvalidConfigException("producer error");
        }
        $this->producer = $rk;
    }
    
    /**
     * 实例化消费者
     * @param string $group
     */
    private function _initConsumer($group)
    {
        $conf = new \RdKafka\Conf();
        $conf->set('group.id', $group);
        $conf->set('metadata.broker.list', $this->brokers);
        
        $topicConf = new \RdKafka\TopicConf();
        $topicConf->set('auto.offset.reset', 'smallest');
        
        $conf->setDefaultTopicConf($topicConf);
        
        $this->consumer = new \RdKafka\KafkaConsumer($conf);
    }
    
    /**
     * 生产数据
     * @param string $topic
     * @param string $message
     * @return boolean
     */
    public function produce($topic, $message)
    {
        if(is_null($this->producer)){
            $this->_initProducer();
        }
        $topicProducer = $this->producer->newTopic($topic);
        return $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $message);
    }
    
    /**
     * 消费数据
     * @param string|array $topic
     * @param string $group
     * @param string $callback
     */
    public function consume($topic, $group, $callback)
    {
        if(is_null($this->consumer)){
            $this->_initConsumer($group);
        }
        $topic = is_array($topic) ? $topic : [$topic];
        $this->consumer->subscribe($topic);
        while(true) {
            $message = $this->consumer->consume(120*1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    call_user_func($callback,$message->payload);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->log("No more messages; will wait for more");
                    sleep(1);
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->log("Timed out");
                    sleep(1);
                    break;
                default:
                    $this->log($message->err. "|" .$message->errstr());
                    break;
            }
        }
    } 
    
    private function log($message)
    {
        $message = date('Y-m-d H:i:s').' | '.trim($message)."\n";
        fwrite(STDERR, $message);
    }
	
}


?>
