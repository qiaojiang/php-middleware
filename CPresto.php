<?php
/*
PrestoClient provides a way to communicate with Presto server REST interface. Presto is a fast query
engine developed by Facebook that runs distributed queries against Hadoop HDFS servers.

Copyright 2013 Xtendsys | xtendsys.net

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

namespace app\common\lib;

class PrestoException extends \Exception {

}

class CPresto {
	/**
	 * The following parameters may be modified depending on your configuration
	 */
	private $source = 'PhpPrestoClient';
	private $version = '0.2';
	private $maximumRetries = 5;
	private $prestoUser = "presto";
	private $prestoSchema = "default";
	private $prestoCatalog = "hive";
	private $userAgent = "";
	
	//Do not modify below this line
	private $nextUri =" ";
	private $infoUri = "";
	private $partialCancelUri = "";
	private $state = "NONE";
	
	private $url;
	private $headers;
	private $result;
	private $request;
	

	public $HTTP_error;
	private $data = array();
	
	//延迟请求时间（毫秒）
	const DELAY_REQUEST_MS = 500000;


	/**
	 * 实例化
	 * @param string $connectUrl
	 * @param string $catalog
	 * @param string $schema
	 */
	public function __construct($connectUrl,$catalog,$schema)
	{
		$this->url = $connectUrl;
		$this->prestoCatalog = $catalog;
		$this->prestoSchema = $schema;
	}

	/**
	 * 查询预处理
	 * 第一次查询无数据返回，需要通过nextUri获取数据
	 *
	 * @param $query
	 * @return bool
	 * @throws Exception
	 */
	private function query($query) 
	{
		$this->data = array();
		$this->userAgent = $this->source."/".$this->version;
		
		$this->request = $query;
		//check that no other queries are already running for this object
		if ($this->state === "RUNNING") {
			return false;
		}
		if (empty($query)) {
		    return false;
		}
		
		$this->headers = array(
			"X-Presto-User: ".$this->prestoUser,
			"X-Presto-Catalog: ".$this->prestoCatalog,
			"X-Presto-Schema: ".$this->prestoSchema,
			"User-Agent: ".$this->userAgent
		);
		
		$connect = \curl_init();
		\curl_setopt($connect,CURLOPT_URL, $this->url);
		\curl_setopt($connect,CURLOPT_HTTPHEADER, $this->headers);
		\curl_setopt($connect,CURLOPT_RETURNTRANSFER, 1);
		\curl_setopt($connect,CURLOPT_POST, 1);
		\curl_setopt($connect,CURLOPT_POSTFIELDS, $this->request);
		$this->result = \curl_exec($connect);

		$httpCode = \curl_getinfo($connect, CURLINFO_HTTP_CODE);
		curl_close($connect);
		if($httpCode != "200"){
			$this->HTTP_error = $httpCode;
			throw new PrestoException("HTTP ERRROR: $this->HTTP_error");
			return false;
		}
		
		$this->parseResult();
		return true;	
	}
	
	/**
	 * 判断是否有数据集
	 * @return bool
	 */
	public function hasNext()
	{
	    if(!empty($this->nextUri)){
	        return true;
	    }else{
	        return false;
	    }
	}
	
	/**
	 * 获取下个结果集
	 * @return array
	 */
	public function getNextRows()
	{
	    $this->data = array();
	    $this->result = file_get_contents($this->nextUri);
	    $this->parseResult();
	    return $this->data;
	}
	
	/**
	 * 解析结果
	 */
	private function parseResult()
	{
	    $decodedJson = json_decode($this->result);

	    if (isset($decodedJson->{'nextUri'})){
	        $this->nextUri = $decodedJson->{'nextUri'};
	    } else {
	        $this->nextUri = false;
	    }
	    
	    if (isset($decodedJson->{'data'})){
	        $this->data = array_merge($this->data,$decodedJson->{'data'});
	    }
	    
	    if (isset($decodedJson->{'infoUri'})){
	        $this->infoUri = $decodedJson->{'infoUri'};
	    }
	    
	    if (isset($decodedJson->{'partialCancelUri'})){
	        $this->partialCancelUri = $decodedJson->{'partialCancelUri'};
	    }
	    
	    if (isset($decodedJson->{'stats'})){
	        $status = $decodedJson->{'stats'};
	        $this->state = $status->{'state'};
	    }
	}

	/**
	 * 查询所有数据
	 *
	 * @return array
	 * @throws PrestoException
	 */
	public function queryAll($sql) 
	{
	    $this->query($sql);
	    
	    $data = array();
		while ($this->hasNext()){
		    usleep(self::DELAY_REQUEST_MS);
			$rows = $this->getNextRows();
			$data = array_merge($data, $rows);
		}
		
		if ($this->state != "FINISHED"){
			throw new PrestoException("Incoherent State at end of query");
			return array();
		}
		return $data;
	}
	
	/**
	 * 查询所有数据并写入日志文件
	 * @param string $sql
	 * @param string $file
	 * @param string $separator
	 * @throws PrestoException
	 * @return boolean
	 */
	public function queryAllToFile($sql, $file, $separator = "\t")
	{
	    $this->query($sql);
	    
	    $data = array();
	    while ($this->hasNext()){
	        usleep(self::DELAY_REQUEST_MS);
	        $rows = $this->getNextRows();
	        if(empty($rows)) continue;
	        foreach ($rows as $row){
	            file_put_contents($file, implode($separator, $row)."\n",FILE_APPEND);
	        }
	    }
	    
	    if ($this->state != "FINISHED"){
	        throw new PrestoException("Incoherent State at end of query");
	        return false;
	    }
	    return true;
	}
	
	/**
	 * 执行SQL
	 * @param string $sql
	 * @throws PrestoException
	 * @return boolean
	 */
	public function execute($sql)
	{
	    $this->query($sql);
	    
	    $data = array();
	    while ($this->hasNext()){
	        usleep(self::DELAY_REQUEST_MS);
	        $this->getNextRows();
	    }
	    
	    if ($this->state != "FINISHED"){
	        throw new PrestoException("Incoherent State at end of query");
	        return false;
	    }
	    return true;
	}
	
	/** 
	 * 获取查询执行信息
	 * @return string
	 */
	public function getInfo() 
	{
		$connect = \curl_init();
        \curl_setopt($connect,CURLOPT_URL, $this->infoUri);
        \curl_setopt($connect,CURLOPT_HTTPHEADER, $this->headers);
		$infoRequest = \curl_exec($connect);
		\curl_close($connect);
		
		return $infoRequest;
	}
	
	/**
	 * 取消请求
	 */
	protected function cancel()
	{
		if (!isset($this->partialCancelUri)){
			return false; 
		}	
		$connect = \curl_init();
		\curl_setopt($connect,CURLOPT_URL, $this->partialCancelUri);
		\curl_setopt($connect,CURLOPT_HTTPHEADER, $this->headers);
		$infoRequest = \curl_exec($connect);
		\curl_close($connect);
		
		$httpCode = \curl_getinfo($connect, CURLINFO_HTTP_CODE);
		if($httpCode != "204"){
			return false;
		}else{
		  return true;
		}
	}
	
}

?>
