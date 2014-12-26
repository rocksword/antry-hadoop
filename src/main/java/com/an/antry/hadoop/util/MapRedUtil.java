package com.an.antry.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class MapRedUtil {

    /**
     * binary search sorted input file and find a line start with target.
     * 
     * @param raf
     *            file to search.
     * @param target
     *            content to search
     * @param forIpRange
     *            true: Sapm IP range search
     * @return
     * @throws IOException
     */
    public static String binarySearch(RandomAccessFile raf, String target, boolean forIpRange) throws IOException {
        raf.seek(0);
        String line = raf.readLine();
        if (forIpRange) {
            if (isTargetIpRange(line, target)) {
                return line;
            }
        } else {
            if (line.startsWith(target)) {
                return line;
            }
        }

        long low = 0;
        long high = raf.length();
        long eof = high;
        long p = -1;
        while (low < high) {
            long mid = (low + high) >>> 1;
            p = mid;
            while (true) {
                raf.seek(p);
                if ((char) raf.readByte() == '\n' || p == 0) {
                    break;
                }
                p--;
            }

            line = raf.readLine();
            if (forIpRange) {
                if (isTargetIpRange(line, target)) {
                    return line;
                }
            }

            if ((line == null) || line.compareTo(target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        p = low;
        if (p == eof) {
            return null;
        }

        while (true) {
            raf.seek(p);
            if (((char) raf.readByte()) == '\n' || p == 0) {
                break;
            }
            p--;
        }

        line = raf.readLine();
        if (forIpRange) {
            if (isTargetIpRange(line, target)) {
                return line;
            }
        } else {
            if (line != null && line.startsWith(target)) {
                return line;
            }
        }
        return null;
    }

    /**
     * @param line
     * @param ipStr
     * @return
     */
    private static boolean isTargetIpRange(String line, String ipStr) {
        if (line != null) {
            String[] strs = line.split(",");
            if (strs.length >= 2) {
                if (ipStr.compareTo(strs[0]) >= 0 && ipStr.compareTo(strs[1]) <= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * binary search sorted input file and find a line.
     * 
     * @param file
     *            file to search.
     * @param target
     *            content to search
     * @return
     * @throws IOException
     */
    public static String binarySearch(RandomAccessFile raf, String target) throws IOException {
        raf.seek(0);
        String line = raf.readLine();
        if (isTarget(line, target)) {
            return line;
        }

        long low = 0;
        long high = raf.length();
        long eof = high;
        long p = -1;
        while (low < high) {
            long mid = (low + high) >>> 1;
            p = mid;

            while (true) {
                raf.seek(p);
                if ((char) raf.readByte() == '\n' || p == 0) {
                    break;
                }
                p--;
            }

            line = raf.readLine();
            if (isTarget(line, target)) {
                return line;
            }
            if ((line == null) || line.compareTo(target) < 0) {
                low = mid + 1;
            } else if (line.compareTo(target) > 0) {
                high = mid;
            }
        }

        p = low;
        if (p == eof) {
            return null;
        }

        while (true) {
            raf.seek(p);
            if (((char) raf.readByte()) == '\n' || p == 0) {
                break;
            }
            p--;
        }

        line = raf.readLine();
        if (!isTarget(line, target)) {
            return null;
        }
        return line;
    }

    /**
     * @param line
     * @param target
     * @return
     */
    private static boolean isTarget(String line, String target) {
        if (line != null) {
            String[] strs = line.split(",");
            if (strs.length >= 2) {
                if (target.compareTo(strs[0]) >= 0 && target.compareTo(strs[1]) <= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Search one line from a file by target.
     * 
     * @param file
     *            file to search.
     * @param ipStr
     *            content to search
     * @return
     * @throws IOException
     */
    private static String[] searchLine(RandomAccessFile raf, String ipStr) throws IOException {
        if (raf == null) {
            return null;
        }
        raf.seek(0);
        String line = raf.readLine();
        String[] target = getTarget(line, ipStr);
        if (target != null) {
            return target;
        }

        long low = 0;
        long high = raf.length();
        long eof = high;
        long p = -1;
        while (low < high) {
            long mid = (low + high) >>> 1;
            p = mid;

            while (true) {
                raf.seek(p);
                if ((char) raf.readByte() == '\n' || p == 0) {
                    break;
                }
                p--;
            }

            line = raf.readLine();
            target = getTarget(line, ipStr);
            if (target != null) {
                return target;
            }
            if ((line == null) || line.compareTo(ipStr) < 0) {
                low = mid + 1;
            } else if (line.compareTo(ipStr) > 0) {
                high = mid;
            }
        }

        p = low;
        if (p == eof) {
            return null;
        }

        while (true) {
            raf.seek(p);
            if (((char) raf.readByte()) == '\n' || p == 0) {
                break;
            }
            p--;
        }

        line = raf.readLine();
        target = getTarget(line, ipStr);
        if (target != null) {
            return target;
        }
        return null;
    }

    /**
     * @param line
     * @param ipStr
     * @return
     */
    private static String[] getTarget(String line, String ipStr) {
        if (line != null) {
            String[] strs = line.split(",");
            if (strs.length == 4) {
                if (ipStr.compareTo(strs[0]) >= 0 && ipStr.compareTo(strs[1]) <= 0) {
                    return new String[] { strs[2], strs[3] };
                }
            }
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(new File("D:\\data\\iploc.csv"), "r");
        System.out.println(binarySearch(raf, "0016777216"));
        System.out.println(binarySearch(raf, "0016815360"));

        String[] target = searchLine(raf, "0016777216");
        if (target != null) {
            System.out.println(target[0] + ", " + target[1]);
        }
        target = searchLine(raf, "0016815360");
        if (target != null) {
            System.out.println(target[0] + ", " + target[1]);
        }

        raf = new RandomAccessFile(new File("D:\\data\\geoloc.csv"), "r");
        System.out.println("Search from range.");
        String line = binarySearch(raf, "0016777472", true);
        System.out.println("Line: " + line);
    }
}
