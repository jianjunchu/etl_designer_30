package org.pentaho.di.trans.steps.pdfreader.common;

import com.xgn.common.Util;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class PDFReader  {
    FileObject file;
    private String title;
    private String bodyText;
    private PDDocument document = null;

    public PDFReader(FileObject var1) throws FileSystemException, IOException {
        this.file = var1;
        this.document = PDDocument.load(Util.getFile(var1));
    }

    public String getContent() throws Exception {
        return this.getContent(-1);
    }

    public int getPageCount() {
        return this.document.getPageCount();
    }

    public String getOnePageContent(int var1) {
        String var2 = null;

        try {
            PDFTextStripper var3 = new PDFTextStripper();
            var3.setSortByPosition(true);
            var3.setStartPage(var1);
            var3.setStartPage(var1 + 1);
            var2 = var3.getText(this.document);
        } catch (Exception var4) {
            var4.printStackTrace();
        } catch (Throwable var5) {
            var5.printStackTrace();
        }

        return var2;
    }

    public void close() {
        if (this.document != null) {
            try {
                this.document.close();
            } catch (IOException var2) {
                var2.printStackTrace();
            }
        }

    }

    public String getContent(int var1) throws Exception {
        if (this.bodyText != null) {
            return this.bodyText;
        } else {
            byte var2 = 1;
            Object var3 = null;
            String var4 = "C:\\test.txt";
            OutputStreamWriter var5 = null;

            try {
                var5 = new OutputStreamWriter(new FileOutputStream(var4));
                PDFTextStripper var6 = new PDFTextStripper();
                var6.setSortByPosition(true);
                var6.setStartPage(var2);
                if (var1 > 0) {
                    var6.setEndPage(var1);
                }

                var6.writeText(this.document, var5);
                this.bodyText = var6.getText(this.document);
            } catch (Exception var11) {
                var11.printStackTrace();
            } catch (Throwable var12) {
                var12.printStackTrace();
            } finally {
                if (this.document != null) {
                    this.document.close();
                }

            }

            return this.bodyText;
        }
    }

    private static COSDocument parseDocument(InputStream var0) throws IOException {
        PDFParser var1 = new PDFParser(var0);
        var1.parse();
        return var1.getDocument();
    }

    private void closeCOSDocument(COSDocument var1) {
        if (var1 != null) {
            try {
                var1.close();
            } catch (IOException var3) {
                ;
            }
        }

    }

    private void closePDDocument(PDDocument var1) {
        if (var1 != null) {
            try {
                var1.close();
            } catch (IOException var3) {
                ;
            }
        }

    }

    public BufferedReader getBufferedReader() throws Exception {
        if (this.bodyText == null || this.bodyText.length() == 0) {
            this.bodyText = this.getContent();
        }

        return new BufferedReader(new StringReader(this.bodyText));
    }

    public BufferedReader getBufferedReader(int var1) throws Exception {
        String var2 = this.getContent(var1);
        return var2 != null && var2.length() != 0 ? new BufferedReader(new StringReader(var2)) : null;
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String var1) {
        this.title = var1;
    }
}
