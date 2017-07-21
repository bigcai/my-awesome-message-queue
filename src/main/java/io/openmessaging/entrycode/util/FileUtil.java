package io.openmessaging.entrycode.util;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by nbs on 2017/5/6.
 */
public class FileUtil {

    /**
     * 删除文件，可以删除单个文件或文件夹
     *
     * @param fileName 被删除的文件名
     * @return 如果删除成功，则返回true，否是返回false
     */
    public static boolean delFile(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            return true;
        } else {
            if (file.isFile()) {
                return FileUtil.deleteFile(fileName);
            } else {
                return FileUtil.deleteDirectory(fileName);
            }
        }
    }

    /**
     * 删除单个文件
     *
     * @param file 被删除的文件名
     * @return 如果删除成功，则返回true，否则返回false
     */
    public static boolean deleteFile(File file) {
        if (null == file) {
            return false;
        }

        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * 删除单个文件
     *
     * @param fileName 被删除的文件名
     * @return 如果删除成功，则返回true，否则返回false
     */
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * 删除目录及目录下的文件
     *
     * @param dirName 被删除的目录所在的文件路径
     * @return 如果目录删除成功，则返回true，否则返回false
     */
    public static boolean deleteDirectory(String dirName) {
        String dirNames = dirName;
        if (!dirNames.endsWith(File.separator)) {
            dirNames = dirNames + File.separator;
        }
        File dirFile = new File(dirNames);
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return true;
        }
        boolean flag = true;
        // 列出全部文件及子目录
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            // 删除子文件
            if (files[i].isFile()) {
                flag = FileUtil.deleteFile(files[i].getAbsolutePath());
                // 如果删除文件失败，则退出循环
                if (!flag) {
                    break;
                }
            }
            // 删除子目录
            else if (files[i].isDirectory()) {
                flag = FileUtil.deleteDirectory(files[i].getAbsolutePath());
                // 如果删除子目录失败，则退出循环
                if (!flag) {
                    break;
                }
            }
        }

        if (!flag) {
            return false;
        }
        // 删除当前目录
        if (dirFile.delete()) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * 创建单个文件
     *
     * @param descFileName 文件名，包含路径
     * @return 如果创建成功，则返回true，否则返回false
     */
    public static boolean createFile(String descFileName) {
        File file = new File(descFileName);
        if (file.exists()) {
            return false;
        }
        if (descFileName.endsWith(File.separator)) {
            return false;
        }
        if (!file.getParentFile().exists()) {
            // 如果文件所在的目录不存在，则创建目录
            if (!file.getParentFile().mkdirs()) {
                return false;
            }
        }

        // 创建文件
        try {
            if (file.createNewFile()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * 创建目录
     *
     * @param descDirName 目录名,包含路径
     * @return 如果创建成功，则返回true，否则返回false
     */
    public static boolean createDirectory(String descDirName) {
        String descDirNames = descDirName;
        if (!descDirNames.endsWith(File.separator)) {
            descDirNames = descDirNames + File.separator;
        }
        File descDir = new File(descDirNames);
        if (descDir.exists()) {
            return false;
        }
        // 创建目录
        if (descDir.mkdirs()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 根据路径返回下面所有文件
     *
     * @param path
     * @return
     */
    public static File[] getFilesByPath(String path) {
        File file = new File(path);
        File[] list = file.listFiles();
        List<File> fileList = new ArrayList<File>();
        for(File f : list){
            if (Pattern.matches(Constant.FILE_FORMATE_PATTERN, f.getName())) {
                fileList.add(f);
            }
        }
        list = new File[fileList.size()];
        return fileList.toArray(list);
    }

    /**
     * 返回路径下包含某个字符的所有文件对象
     *
     * @param path
     * @param name
     * @return
     */
    public static List<File> getFilesByPathAndFilterByName(String path, String name) {
        File file = new File(path);
        if( !file.exists() ) {
        	file.mkdir();
        }
        File[] files = file.listFiles();
        List<File> fileList = new ArrayList<File>();
        String tempName;
        for (File f : files) {
            tempName = f.getName();
            if (Pattern.matches(Constant.FILE_FORMATE_PATTERN, tempName) && tempName.contains(name)) {
                fileList.add(f);
            }
        }
        return fileList;
    }

    /**
     * 获取某个路径下文件名中ID最大的,文件命名规则  数字-业务名称
     *
     * @return
     */
    public static int getMaxIdByPath(String path, String businessName) {
        File[] files = FileUtil.getFilesByPath(path);
        if (files.length > 0) {
            List<File> list = Arrays.asList(files);
            Collections.sort(list,new Comparator<File>(){
                @Override
                public int compare(File b1, File b2) {
                    return Integer.parseInt(b1.getName().substring(0,b1.getName().indexOf("-"))) -
                            Integer.parseInt(b2.getName().substring(0,b2.getName().indexOf("-")));
                }
            });
            String tempName;
            int size = list.size();
            for (int i = 0; i < size; i++) {
                tempName = list.get(size - i - 1).getName();
                if (Pattern.matches(Constant.FILE_FORMATE_PATTERN, tempName) && tempName.contains(businessName) ) {
                    return getIdByName(tempName);
                }
            }
        }
        return 0;
    }

    /**
     * 通过名称获得ID
     *
     * @param name
     * @return
     */
    public static int getIdByName(String name) {
        return Integer.parseInt(name.substring(0, name.indexOf("-")));
    }
    
    public static long getMessageDataPageSize(String path, String businessName) {
		File[] files = FileUtil.getFilesByPath(path);
        if (files.length > 0) {
            List<File> list = Arrays.asList(files);
            Collections.sort(list);
            String tempName;
            int size = list.size();
            for (int i = 0; i < size; i++) {
                tempName = list.get(size - i - 1).getName();
                if (Pattern.matches(Constant.FILE_FORMATE_PATTERN, tempName) && tempName.contains(businessName) ) {
                    return list.get(size - i - 1).length();
                }
            }
        }
		return 0L;
	}

	public static boolean isFileExists(String path){
        File file = new File(path);
        return file.exists();
    }
}
