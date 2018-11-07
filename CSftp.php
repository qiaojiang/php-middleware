<?php
namespace app\common\lib;

use yii\base\Component;

class CSftp extends Component
{
    public $host;
    public $port = 22;
    public $username;
    public $password;
    //是否使用秘钥登陆
    public $use_pubkey_file = false;
    public $pubkey_file;
    public $privkey_file;
    public $passphrase;
    
    //SSH连接
    private $conn; 

    public function init()
    {
        parent::init();
        
        $this->connect();
    }
    
    /**
     * 连接SSH
     * 连接有两种方式 (1)使用秘钥  (2)使用密码 
     */
    public function connect()
    {
        //$methods['hostkey'] = 'ssh-rsa';
        $this->conn = ssh2_connect($this->host, $this->port);
        
        if($this->use_pubkey_file){ //使用秘钥
            $rc = ssh2_auth_pubkey_file($this->conn,$this->username,$this->pubkey_file,$this->privkey_file,$this->passphrase);
        }else{ //使用密码 
            $rc = ssh2_auth_password( $this->conn, $this->username,$this->password);
        }
        return $rc ;
    }
    
    /**
     * 上传数据
     */
    public function upload($local, $remote, $file_mode = 0664)
    {
        return ssh2_scp_send($this->conn, $local, $remote, $file_mode);
    }
    
    /**
     * 下载
     * @param string $remotefile
     * @param string $localfile
     * @return boolean
     */
    public function download($remoteFile, $localFile)
    {
        return ssh2_scp_recv($this->conn, $remoteFile, $localFile);
    }
    
    /**
     * 下载多个文件
     * @param string $remotePath
     * @param string $localPath
     */
    public function downloadFiles($remotePath, $localPath)
    {
        $files = $this->getFiles($remotePath);
        $dirname = dirname($remotePath);
        foreach ($files as $file){
            $basename = basename($file);
            $this->download($dirname."/".$basename, $localPath."/".$basename);
        }
    }
    
    /**
     * 获取路径下的文件
     * @param string $path
     * @return array
     */
    public function getFiles($path)
    {
        $stream = ssh2_exec($this->conn, "ls ".$path);
        stream_set_blocking($stream, true);
        $output = stream_get_contents($stream);
        if(empty($output)) return [];
        return explode("\n", trim($output));
    }
    
    /**
     * 获取目录下的文件数
     * @param string $path
     * @return number
     */
    public function getFileCount($path)
    {
        return count($this->getFiles($path));
    }
    
    /**
     * 删除文件
     */
    public function remove($remote)
    {
        $sftp = ssh2_sftp($this->conn);
        $rc = false;
        if (is_dir("ssh2.sftp://{$sftp}/{$remote}")) {
            $rc = false ;
            // ssh 删除文件夹
            $rc = ssh2_sftp_rmdir($sftp, $remote);
        } else {
            // 删除文件
            $rc = ssh2_sftp_unlink($sftp, $remote);
        }
        return $rc;
    }
    
    public function __destruct()
    {

  
    }
}
