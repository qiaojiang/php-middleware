<?php
/**
 * Hbase类
 */
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/thrift';
require_once $GLOBALS['THRIFT_ROOT'].'/Hbase/Hbase.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Hbase/Hbase_types.php';

require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocketPool.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

class CHbase { 
    
    //主机
    private $host;
    //端口
    private $port;
    //TFramedTransport
    private $transport = null;
    //HbaseClient
    private $client = null;
    
    //scan数据条数默认限制
    const SCAN_LIMIT = 1000;
    
      
    public function __construct($host, $port)
    {
        try {
            $socket = new TSocket($host, $port);
            $socket->setSendTimeout(30000);
            $socket->setRecvTimeout(30000);
            $this->transport = new TFramedTransport($socket);
            $protocol = new TBinaryProtocol($this->transport);
            $this->client = new HbaseClient($protocol);
            $this->transport->open();
        } catch(Exception $e) {
            $msg = "can't connect ".$host.":".$port." exception=".$e->getMessage();
            throw new TException($msg);
        }
    }
    
    /**
     * 处理TCeil
     */
    private function _handleCeil($cell)
    {
        if(empty($cell)) return array();
        return array(
            'value' => $cell->value,
            'timestamp' => $cell->timestamp
        );
        
    }
    
    /**
     * 处理TRowResult
     */
    private function _handleRow($row)
    {
        if(empty($row)) return array();
        $data = array();
        foreach ($row->columns as $key => $cell){
            $d['row'] = $row->row;
            $d['column'] = $key;
            $d['value'] = $cell->value;
            $d['timestamp'] = $cell->timestamp;
            $data[] = $d;
        }
        return $data;
    }
    
    /**
     * 获取单个ceil数据
     * @param string $tableName 表名称
     * @param string $row 行健
     * @param string $column 列（格式： 列族:列）
     */
    public function get($tableName,$row,$column)
    {
        $result = $this->client->get($tableName, $row, $column);
        foreach ($result as &$cell){
            $cell = $this->_handleCeil($cell);
        }
        return $result;
    }
    
    /**
     * 获取单行
     * @param string $tableName 表名称
     * @param string $row 行健
     */
    public function getRow($tableName, $row)
    {
        $rs = $this->client->getRow($tableName, $row);
        $result = array();
        foreach ($rs as $r){
            $row = $this->_handleRow($r);
            $result = array_merge($result,$row);
        }
        return $result;
    }
    
    /**
     * 获取多行
     * @param string $tableName 表名称
     * @param string $rows 行健集合
     */
    public function getRows($tableName, $rows)
    {
        $rs = $this->client->getRows($tableName, $rows);
        $result = array();
        foreach ($rs as $r){
            $result[$r->row] = $this->_handleRow($r);
        }
        return $result;
    }
    
    /**
     * 写单行数据
     * @param string $tableName 表名称
     * @param string $row 行健
     * @param array $mutates  列数据集
     * $mutates = 
     * [
           [
              'column' => $family.":".$column,
              'value' => $data
           ],
        ]
     *
     */
    public function put($tableName, $row, $mutates)
    { 
        $ms = array();
        foreach ($mutates as $m){
            $ms[] = new Mutation($m);
        }
        $this->client->mutateRow($tableName, $row, $ms);
        
    }
    
    /**
     * 批量写
     * @param string $tableName 表名称
     * @param array $rowBatches
     * $rowBatches = 
     * [
           [
                'row' => $rowkey,
                'mutations' => [
                   [
                      'column' => $family.":".$column,
                      'value' => $data
                   ],
                ]
           ]
     * ]
     * 
     */
    public function putRows($tableName, $rowBatches)
    {
        $rbs = array();
        foreach ($rowBatches as $row){
            $ms = array();
            foreach ($row['mutations'] as $m){
                $ms[] = new Mutation($m);
            }
            
            $rbs[] = new BatchMutation(array('row' => $row['row'],'mutations' => $ms));
        }
        $this->client->mutateRows($tableName, $rbs);
    }
    
    /**
     * 扫描表
     * @param string $tableName 表名称
     * @param array $columns    查询列集合（格式： 列族:列）
     * @param array $params     附加条件
     * @return array:
     */
    public function scan($tableName, $columns, $params = array())
    {
        $startRow = isset($params['start_row']) ? $params['start_row'] : '';
        $stopRow = isset($params['stop_row']) ? $params['stop_row'] : '';
        $limit = isset($params['limit']) ? $params['limit'] : self::SCAN_LIMIT;
        $scanner = $this->client->scannerOpenWithStop($tableName,$startRow, $stopRow, $columns);
        $result = array();
        $i = 0;
        while($rs = $this->client->scannerGet($scanner)) {
            if (count($rs) == 0) {
                break;
            }
            foreach ($rs as $r){
                $row = $this->_handleRow($r);
                $result = array_merge($result,$row);
            }
            if(++$i >= $limit) break;
        }
        $this->client->scannerClose($scanner);
        return $result;
    }
    
    public function __destruct()
    {
        try {
            $this->transport->close();
        } catch(Exception $e) {
            Throw new TException("close transport fail");
        }
    }

}     

