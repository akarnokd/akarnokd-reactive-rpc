package hu.akarnokd.reactiverpc;

import java.io.*;

enum RpcHelper {
    
    ;
    
    public static int readFully(InputStream in, byte[] output, int count) throws IOException {
        int offset = 0;
        int remaining = count;
        
        for (;;) {
            int a = in.read(output, offset, remaining);
            if (a < 0) {
                break;
            }
            offset += a;
            remaining -= a;
            if (remaining == 0) {
                break;
            }
        }
        
        return offset;
    }
    
    public static String readUtf8(InputStream in, int payloadLength) throws IOException {
        StringBuilder sb = new StringBuilder(payloadLength);
        
        for (;;) {
            if (payloadLength == 0) {
                break;
            }
            int b = in.read();

            payloadLength--;

            if (b < 0) {
                break;
            } else
            if ((b & 0x80) == 0) {
                sb.append((char)b);
            } else
            if ((b & 0b1110_0000) == 0b1100_0000) {
                
                if (payloadLength-- == 0) {
                    break;
                }
                
                int b1 = in.read();
                if (b1 < 0) {
                    break;
                }
                
                int c = ((b & 0b1_1111) << 6) | (b1 & 0b0011_1111);
                sb.append((char)c);
            } else
            if ((b & 0b1111_0000) == 0b1110_0000) {
                if (payloadLength-- == 0) {
                    break;
                }
                
                int b1 = in.read();

                if (b1 < 0) {
                    break;
                }
                
                if (payloadLength-- == 0) {
                    break;
                }
                
                int b2 = in.read();

                if (b2 < 0) {
                    break;
                }
                
                int c = ((b & 0b1111) << 12) | ((b1 & 0b11_1111) << 6)
                        | ((b2 & 0b11_1111));
                
                sb.append((char)c);
            } else
            if ((b & 0b1111_1000) == 0b1111_0000) {
                if (payloadLength-- == 0) {
                    break;
                }
                
                int b1 = in.read();

                if (b1 < 0) {
                    break;
                }
                
                if (payloadLength-- == 0) {
                    break;
                }
                
                int b2 = in.read();

                if (b2 < 0) {
                    break;
                }

                if (payloadLength-- == 0) {
                    break;
                }
                
                int b3 = in.read();

                if (b3 < 0) {
                    break;
                }

                int c = ((b & 0b111) << 18) 
                        | ((b1 & 0b11_1111) << 12)
                        | ((b1 & 0b11_1111) << 6)
                        | ((b2 & 0b11_1111));
                
                sb.append((char)c);
            }
        }
        
        return sb.toString();
    }
    
    public static String readUtf8(byte[] in, int start, int len) throws IOException {
        StringBuilder sb = new StringBuilder(len);
        
        for (;;) {
            if (start == len) {
                break;
            }
            int b = in[start++] & 0xFF;

            if ((b & 0x80) == 0) {
                sb.append((char)b);
            } else
            if ((b & 0b1110_0000) == 0b1100_0000) {
                
                if (start == len) {
                    break;
                }
                
                int b1 = in[start++] & 0b11_1111;
                
                int c = ((b & 0b1_1111) << 6) | (b1);
                sb.append((char)c);
            } else
            if ((b & 0b1111_0000) == 0b1110_0000) {
                if (start == len) {
                    break;
                }
                int b1 = in[start++] & 0b11_1111;

                if (start == len) {
                    break;
                }

                int b2 = in[start++] & 0b11_1111;

                int c = ((b & 0b1111) << 12) | (b1 << 6)
                        | (b2);
                
                sb.append((char)c);
            } else
            if ((b & 0b1111_1000) == 0b1111_0000) {
                if (start == len) {
                    break;
                }
                int b1 = in[start++] & 0b11_1111;

                if (start == len) {
                    break;
                }

                int b2 = in[start++] & 0b11_1111;

                if (start == len) {
                    break;
                }

                int b3 = in[start++] & 0b11_1111;

                int c = ((b & 0b111) << 18) 
                        | (b1 << 12)
                        | (b2 << 6)
                        | (b3);
                
                sb.append((char)c);
            }
        }
        
        return sb.toString();
    }
}
