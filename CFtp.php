<?php
namespace app\common\lib;

use yii\base\Component;

class CFtp extends Component
{        
    public $host;
    public $port = 21;
    public $username;
    public $password;
    public $pasv = true;
    
    //FTP连接
    private $conn;    
    
    public function init()
    {
        parent::init();

        $this->connect();
    }
    
    /**
     * FTP连接
     */
    public function connect()
    {
        $this->conn = @ftp_connect($this->host, $this->port) or die("FTP服务器连接失败");
        @ftp_login($this->conn, $this->username, $this->password) or die("FTP服务器登陆失败");
        //打开被动模拟
        if($this->pasv) @ftp_pasv($this->conn, 1); 
    }
    
    /**
     * 上传文件
     * @param string path  本地路径
     * @param string  newpath 上传路径
     * @param boolean type 若目标目录不存在则新建
     */
    public function upload($localFile, $remoteFile, $type=true)
    {
        if($type) $this->mkdirs($remoteFile);
        return @ftp_put($this->conn, $remoteFile, $localFile, FTP_BINARY);
    }
    
    /**
     * 移动文件
     * @param string path  原路径
     * @param string newpath 新路径
     * @param boolean type  若目标目录不存在则新建
     */
    public function move($oldname, $newname, $type=true)
    {
        if($type) $this->mkdirs($newname);
        return @ftp_rename($this->conn, $oldname, $newname);
    }
    
    /**
     * 复制文件
     * 说明：由于FTP无复制命令,本方法变通操作为：下载后再上传到新的路径
     * @param string path  原路径
     * @param string newpath 新路径
     * @param boolean type 若目标目录不存在则新建
     */
    public function copy($path, $newpath, $type=true)
    {
        $tmpFile = "/tmp/".uniqid("ftp_").".log";
        $status = @ftp_get($this->conn, $tmpFile, $path, FTP_BINARY);
        if(!$status) return false;
        $status = $this->upload($tmpFile, $newpath, $type);
        unlink($tmpFile);
        return $status;
    }
    
    /**
     * 下载
     * @param string $remotefile
     * @param string $localfile
     * @return boolean
     */
    public function download($remoteFile, $localFile)
    {
        if(!is_file($localFile)){
            touch($localFile);
        }
        return @ftp_get($this->conn, $localFile, $remoteFile, FTP_BINARY);
    }
    
    /**
     * 下载多个文件
     * @param string $remotePath
     * @param string $localPath
     */
    public function downloadFiles($remotePath, $localPath)
    {
        $files = @ftp_nlist($this->conn, $remotePath);
        foreach ($files as $file){
            $filename = basename($file);
            $this->download($file, $localPath."/".$filename);
        }
    }
    
    /**
     * 获取路径下的文件
     * @param string $path
     * @return array
     */
    public function getFiles($path)
    {
        return @ftp_nlist($this->conn, $path);
    }
    
    /**
     * 获取目录下文件数
     * @param string $path
     * @return int
     */
    public function getFileCount($path)
    {
        $files = $this->getFiles($path);
        return count($files);
    }
    
    /**
     * 删除文件
     * @param string path 路径
     */
    public function remove($path)
    {
        return @ftp_delete($this->conn, $path);
    }
    
    /**
     * 生成目录
     * @param string path 路径
     */
    public function mkdirs($path)
    {
        $path_arr = explode('/',$path);       // 取目录数组
        $file_name = array_pop($path_arr);      // 弹出文件名
        $path_div = count($path_arr);        // 取层数
        
        foreach($path_arr as $val){
            if(@ftp_chdir($this->conn, $val) == FALSE){
                $tmp = @ftp_mkdir($this->conn, $val);
                if(!$tmp){
                    return false;
                }
                @ftp_chdir($this->conn, $val);
            }
        }
        //回退到根
        for($i = 1; $i <= $path_div; $i++){
            @ftp_cdup($this->conn);
        }
        return true;
    }
    
    /**
     * 方法：关闭FTP连接
     */
    public function close()
    {
        return @ftp_close($this->conn);
    }
}
?>