import java.io.InputStream;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws Exception {
        FastScanner sc = new FastScanner(System.in);
        int t = sc.nextInt();
        if (t == -1) return;
        
        StringBuilder out = new StringBuilder();
        while (t-- > 0) {
            int n = sc.nextInt();
            int[] a = new int[n];
            for (int i = 0; i < n; i++) {
                a[i] = sc.nextInt();
            }
            
            Arrays.sort(a);
            int pairs = 0;
            for (int i = 0; i < n - 1; i++) {
                if (a[i] == a[i + 1]) {
                    pairs++;
                    i++;
                }
            }
            
            out.append(Math.min(pairs, (n - 1) / 2)).append("\n");
        }
        System.out.print(out);
    }
    
    static class FastScanner {
        private InputStream stream;
        private byte[] buf = new byte[1 << 16];
        private int head, tail;

        public FastScanner(InputStream stream) {
            this.stream = stream;
        }

        private int read() {
            if (head >= tail) {
                head = 0;
                try {
                    tail = stream.read(buf, 0, buf.length);
                } catch (Exception e) {
                    return -1;
                }
                if (tail <= 0) return -1;
            }
            return buf[head++];
        }

        public int nextInt() {
            int c = read();
            while (c <= 32) {
                if (c == -1) return -1;
                c = read();
            }
            int res = 0;
            while (c > 32) {
                res = res * 10 + c - '0';
                c = read();
            }
            return res;
        }
    }
}
