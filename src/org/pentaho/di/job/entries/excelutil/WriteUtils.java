package org.pentaho.di.job.entries.excelutil;

import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.*;
import org.openxmlformats.schemas.drawingml.x2006.spreadsheetDrawing.CTMarker;
import org.openxmlformats.schemas.drawingml.x2006.spreadsheetDrawing.CTTwoCellAnchor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 * source: https://coderanch.com/t/420958/open-source/Copying-sheet-excel-file-excel
 * @author jk
 * getted from http://jxls.cvs.sourceforge.net/jxls/jxls/src/java/org/jxls/util/Util.java?revision=1.8&view=markup
 * by Leonid Vysochyn
 * and modified (adding styles copying)
 * modified by Philipp Löpmeier (replacing deprecated classes and methods, using generic types)
 * modified by a.filipchik (xlsx support & optimizations)
 */
public final class WriteUtils {

    public static void copySheet(XSSFSheet source, XSSFSheet destination){
        copySheet(source, destination, true);
    }

    private static void copySheet(XSSFSheet source, XSSFSheet destination, boolean copyStyle) {
        int maxColumnNum = 0;
        List<CellStyle> styleMap2 = (copyStyle) ? new ArrayList<CellStyle>() : null;
        for (int i = source.getFirstRowNum(); i <= source.getLastRowNum(); i++) {
            XSSFRow srcRow = source.getRow(i);
            XSSFRow destRow = destination.createRow(i);
            if (srcRow != null) {
                copyRow(source, destination, srcRow, destRow, styleMap2);
                if (srcRow.getLastCellNum() > maxColumnNum) {
                    maxColumnNum = srcRow.getLastCellNum();
                }
            }
        }
        for (int i = 0; i <= maxColumnNum; i++) {
            destination.setColumnWidth(i, source.getColumnWidth(i));
        }
    }

    /**
     * @param srcSheet
     *            the sheet to copy.
     * @param destSheet
     *            the sheet to create.
     * @param srcRow
     *            the row to copy.
     * @param destRow
     *            the row to create.
     * @param styleMap
     *            -
     */
    private static void copyRow(XSSFSheet srcSheet, XSSFSheet destSheet, XSSFRow srcRow, XSSFRow destRow,
                                List<CellStyle> styleMap) {
        Set<CellRangeAddressWrapper> mergedRegions = new TreeSet<CellRangeAddressWrapper>();
        short dh = srcSheet.getDefaultRowHeight();
        if (srcRow.getHeight() != dh) {
            destRow.setHeight(srcRow.getHeight());
        }


        int j = srcRow.getFirstCellNum();
        if (j < 0) {
            j = 0;
        }
        for (; j <= srcRow.getLastCellNum(); j++) {
            XSSFCell oldCell = srcRow.getCell(j); // ancienne cell
            XSSFCell newCell = destRow.getCell(j); // new cell
            if (oldCell != null) {
                if (newCell == null) {
                    newCell = destRow.createCell(j);
                }

                copyCell(oldCell, newCell, styleMap);
                CellRangeAddress mergedRegion = getMergedRegion(srcSheet, srcRow.getRowNum(),
                        (short) oldCell.getColumnIndex());

                if (mergedRegion != null) {
                    CellRangeAddress newMergedRegion = new CellRangeAddress(mergedRegion.getFirstRow(),
                            mergedRegion.getLastRow(), mergedRegion.getFirstColumn(), mergedRegion.getLastColumn());
                    CellRangeAddressWrapper wrapper = new CellRangeAddressWrapper(newMergedRegion);
                    if (isNewMergedRegion(wrapper, mergedRegions)) {
                        mergedRegions.add(wrapper);
                        destSheet.addMergedRegion(wrapper.range);
                    }
                }
            }
        }

    }
    public static CellRangeAddress getMergedRegion(XSSFSheet sheet, int rowNum, short cellNum) {
        for (int i = 0; i < sheet.getNumMergedRegions(); i++) {
            CellRangeAddress merged = sheet.getMergedRegion(i);
            if (merged.isInRange(rowNum, cellNum)) {
                return merged;
            }
        }
        return null;
    }
    private static boolean isNewMergedRegion(CellRangeAddressWrapper newMergedRegion,
                                             Set<CellRangeAddressWrapper> mergedRegions) {
        return !mergedRegions.contains(newMergedRegion);
    }

    private static void copyPrintTitle(Sheet newSheet, Sheet sheetToCopy) {
        int nbNames = sheetToCopy.getWorkbook().getNumberOfNames();
        Name name = null;
        String formula = null;

        String part1S = null;
        String part2S = null;
        String formS = null;
        String formF = null;
        String part1F = null;
        String part2F = null;
        int rowB = -1;
        int rowE = -1;
        int colB = -1;
        int colE = -1;

        for (int i = 0; i < nbNames; i++) {
            name = sheetToCopy.getWorkbook().getNameAt(i);
            if (name.getSheetIndex() == sheetToCopy.getWorkbook().getSheetIndex(sheetToCopy)) {
                if (name.getNameName().equals("Print_Titles")
                        || name.getNameName().equals(XSSFName.BUILTIN_PRINT_TITLE)) {
                    formula = name.getRefersToFormula();
                    int indexComma = formula.indexOf(",");
                    if (indexComma == -1) {
                        indexComma = formula.indexOf(";");
                    }
                    String firstPart = null;
                    ;
                    String secondPart = null;
                    if (indexComma == -1) {
                        firstPart = formula;
                    } else {
                        firstPart = formula.substring(0, indexComma);
                        secondPart = formula.substring(indexComma + 1);
                    }

                    formF = firstPart.substring(firstPart.indexOf("!") + 1);
                    part1F = formF.substring(0, formF.indexOf(":"));
                    part2F = formF.substring(formF.indexOf(":") + 1);

                    if (secondPart != null) {
                        formS = secondPart.substring(secondPart.indexOf("!") + 1);
                        part1S = formS.substring(0, formS.indexOf(":"));
                        part2S = formS.substring(formS.indexOf(":") + 1);
                    }

                    rowB = -1;
                    rowE = -1;
                    colB = -1;
                    colE = -1;
                    String rowBs, rowEs, colBs, colEs;
                    if (part1F.lastIndexOf("$") != part1F.indexOf("$")) {
                        rowBs = part1F.substring(part1F.lastIndexOf("$") + 1, part1F.length());
                        rowEs = part2F.substring(part2F.lastIndexOf("$") + 1, part2F.length());
                        rowB = Integer.parseInt(rowBs);
                        rowE = Integer.parseInt(rowEs);
                        if (secondPart != null) {
                            colBs = part1S.substring(part1S.lastIndexOf("$") + 1, part1S.length());
                            colEs = part2S.substring(part2S.lastIndexOf("$") + 1, part2S.length());
                            colB = CellReference.convertColStringToIndex(colBs);// CExportExcelHelperPoi.convertColumnLetterToInt(colBs);
                            colE = CellReference.convertColStringToIndex(colEs);// CExportExcelHelperPoi.convertColumnLetterToInt(colEs);
                        }
                    } else {
                        colBs = part1F.substring(part1F.lastIndexOf("$") + 1, part1F.length());
                        colEs = part2F.substring(part2F.lastIndexOf("$") + 1, part2F.length());
                        colB = CellReference.convertColStringToIndex(colBs);// CExportExcelHelperPoi.convertColumnLetterToInt(colBs);
                        colE = CellReference.convertColStringToIndex(colEs);// CExportExcelHelperPoi.convertColumnLetterToInt(colEs);

                        if (secondPart != null) {
                            rowBs = part1S.substring(part1S.lastIndexOf("$") + 1, part1S.length());
                            rowEs = part2S.substring(part2S.lastIndexOf("$") + 1, part2S.length());
                            rowB = Integer.parseInt(rowBs);
                            rowE = Integer.parseInt(rowEs);
                        }
                    }

//                    newSheet.getWorkbook().setRepeatingRowsAndColumns(newSheet.getWorkbook().getSheetIndex(newSheet),
                    //                           colB, colE, rowB - 1, rowE - 1);
                }
            }
        }
    }

    public static final String dir_1 =
            "C:" + File.separator
                    + "Users" + File.separator
                    + "x" + File.separator
                    + "x" + File.separator
                    + "x" + File.separator
                    + "x.xlsx";

    public static final String dir_2 =
            "C:" + File.separator
                    + "Users" + File.separator
                    + "x" + File.separator
                    + "x" + File.separator
                    + "x" + File.separator
                    + "x2.xlsx";

    public static void main(String[] args) throws IOException, InvalidFormatException {

        File excel_1 = new File(dir_1);
        File excel_2 = new File(dir_2);
        Workbook wb_1 = new XSSFWorkbook(OPCPackage.open(dir_1));
        Workbook wb_2 = new XSSFWorkbook(OPCPackage.open(dir_2));

        wb_1.setSheetName(0, "Actual");
        wb_1.createSheet("Last Week");

        /*Get sheets from the temp file*/
        XSSFSheet sheet_1 = ((XSSFWorkbook) wb_1).getSheet("Last Week");
        XSSFSheet sheet_2 = ((XSSFWorkbook) wb_2).getSheetAt(0);



        copySheet(sheet_2, sheet_1);

        OutputStream os = new FileOutputStream("hello.xlsx");

        System.out.print ("If you arrived here, it means you're good boy");
        wb_1.write(os);
        os.flush();
        os.close();
        wb_1.close();
    }
    public static void copyCell(HSSFCell oldCell, HSSFCell newCell, Map<Integer, HSSFCellStyle> styleMap) {

        if (styleMap != null) {
            if (oldCell.getSheet().getWorkbook() == newCell.getSheet().getWorkbook()) {
                newCell.setCellStyle(oldCell.getCellStyle());
            } else {
                int stHashCode = oldCell.getCellStyle().hashCode();
                HSSFCellStyle newCellStyle = styleMap.get(stHashCode);
                if (newCellStyle == null) {
                    newCellStyle = newCell.getSheet().getWorkbook().createCellStyle();
                    newCellStyle.cloneStyleFrom(oldCell.getCellStyle());
                    styleMap.put(stHashCode, newCellStyle);
                }
                newCell.setCellStyle(newCellStyle);
            }
        }
        switch (oldCell.getCellType()) {
            case STRING:
                newCell.setCellValue(oldCell.getStringCellValue());
                // System.out.println("oldCell = " + oldCell.getStringCellValue());
                // System.out.println("newCell = " + newCell.getStringCellValue());

                break;
            case NUMERIC:
                newCell.setCellValue(oldCell.getNumericCellValue());
                // System.out.println("oldCell = " + oldCell.getNumericCellValue());
                // System.out.println("newCell = " + newCell.getNumericCellValue());
                break;
            case BLANK:
                newCell.setCellType(CellType.BLANK);
                break;
            case BOOLEAN:
                newCell.setCellValue(oldCell.getBooleanCellValue());
                break;
            case ERROR:
                newCell.setCellErrorValue(oldCell.getErrorCellValue());
                break;
            case FORMULA:
                newCell.setCellFormula(oldCell.getCellFormula());
                break;
            default:
                break;
        }
    }


//    public static void copyHSSFSheets(HSSFWorkbook sourceWB, HSSFWorkbook destinationWB) {
//        for (Iterator<Sheet> it = sourceWB.sheetIterator(); it.hasNext(); ) {
//            HSSFSheet sheet = (HSSFSheet) it.next();
//            String sheetName = sheet.getSheetName();
//            if (destinationWB.getSheetIndex(sheetName) != -1) {
//                int index = 1;
//                while (destinationWB.getSheetIndex(sheetName + "(" + index + ")") != -1) {
//                    index++;
//                }
//                sheetName += "(" + index + ")";
//            }
//            HSSFSheet newSheet = destinationWB.createSheet(sheetName);
//            copySheetSettings(newSheet, sheet);
//            copyHSSFSheet(newSheet, sheet);
//            copyPictures(newSheet, sheet);
//        }
//    }
//
//    public static void copyHSSFSheet(HSSFSheet newSheet, HSSFSheet sheet) {
//        int maxColumnNum = 0;
//        Map<Integer, HSSFCellStyle> styleMap = new HashMap<>();
//        // manage a list of merged zone in order to not insert two times a merged zone
//        Set<String> mergedRegions = new TreeSet<>();
//        List<CellRangeAddress> sheetMergedRegions = sheet.getMergedRegions();
//        for (int i = sheet.getFirstRowNum(); i <= sheet.getLastRowNum(); i++) {
//            HSSFRow srcRow = sheet.getRow(i);
//            HSSFRow destRow = newSheet.createRow(i);
//            if (srcRow != null) {
//                copyHSSFRow(newSheet, srcRow, destRow, styleMap, sheetMergedRegions, mergedRegions);
//                if (srcRow.getLastCellNum() > maxColumnNum) {
//                    maxColumnNum = srcRow.getLastCellNum();
//                }
//            }
//        }
//        for (int i = 0; i <= maxColumnNum; i++) {
//            newSheet.setColumnWidth(i, sheet.getColumnWidth(i));
//        }
//    }

//    public static void copyHSSFRow(HSSFSheet destSheet, HSSFRow srcRow, HSSFRow destRow, Map<Integer, HSSFCellStyle> styleMap, List<CellRangeAddress> sheetMergedRegions, Set<String> mergedRegions) {
//        destRow.setHeight(srcRow.getHeight());
//        // pour chaque row
//        for (int j = srcRow.getFirstCellNum(); j <= srcRow.getLastCellNum(); j++) {
//            HSSFCell oldCell = srcRow.getCell(j);   // ancienne cell
//            HSSFCell newCell = destRow.getCell(j);  // new cell
//            if (oldCell != null) {
//                if (newCell == null) {
//                    newCell = destRow.createCell(j);
//                }
//                // copy chaque cell
//                copyHSSFCell(oldCell, newCell, styleMap);
//                // copy les informations de fusion entre les cellules
//                CellRangeAddress mergedRegion = getMergedRegion(sheetMergedRegions, srcRow.getRowNum(), (short) oldCell.getColumnIndex());
//
//                if (mergedRegion != null) {
//                    CellRangeAddress newMergedRegion = new CellRangeAddress(mergedRegion.getFirstRow(), mergedRegion.getLastRow(), mergedRegion.getFirstColumn(), mergedRegion.getLastColumn());
//                    if (isNewMergedRegion(newMergedRegion, mergedRegions)) {
//                        mergedRegions.add(newMergedRegion.formatAsString());
//                        destSheet.addMergedRegion(newMergedRegion);
//                    }
//                }
//            }
//        }
//
//    }

    public static void copyHSSFCell(HSSFCell oldCell, HSSFCell newCell, Map<Integer, HSSFCellStyle> styleMap) {
        if (styleMap != null) {
            if (oldCell.getSheet().getWorkbook() == newCell.getSheet().getWorkbook()) {
                newCell.setCellStyle(oldCell.getCellStyle());
            } else {
                int stHashCode = oldCell.getCellStyle().hashCode();
                HSSFCellStyle newCellStyle = styleMap.get(stHashCode);
                if (newCellStyle == null) {
                    newCellStyle = newCell.getSheet().getWorkbook().createCellStyle();
                    newCellStyle.cloneStyleFrom(oldCell.getCellStyle());
                    styleMap.put(stHashCode, newCellStyle);
                }
                newCell.setCellStyle(newCellStyle);
            }
        }
        switch (oldCell.getCellTypeEnum()) {
            case STRING:
                newCell.setCellValue(oldCell.getStringCellValue());
                break;
            case NUMERIC:
                newCell.setCellValue(oldCell.getNumericCellValue());
                break;
            case BLANK:
                newCell.setCellType(CellType.BLANK);
                break;
            case BOOLEAN:
                newCell.setCellValue(oldCell.getBooleanCellValue());
                break;
            case ERROR:
                newCell.setCellErrorValue(oldCell.getErrorCellValue());
                break;
            case FORMULA:
                newCell.setCellFormula(oldCell.getCellFormula());
                break;
            default:
                break;
        }
    }

    public static void copyCell(Cell oldCell, Cell newCell, List<CellStyle> styleList) {
        if (styleList != null) {
            if (oldCell.getSheet().getWorkbook() == newCell.getSheet().getWorkbook()) {
                newCell.setCellStyle(oldCell.getCellStyle());
            } else {
                DataFormat newDataFormat = newCell.getSheet().getWorkbook().createDataFormat();

                CellStyle newCellStyle = getSameCellStyle(oldCell, newCell, styleList);
                if (newCellStyle == null) {
                    //Create a new cell style
                    Font oldFont = oldCell.getSheet().getWorkbook().getFontAt(oldCell.getCellStyle().getFontIndex());
                    //Find a existing font corresponding to avoid to create a new one
                    Font newFont = newCell.getSheet().getWorkbook().findFont(oldFont.getBold(), oldFont.getColor(), oldFont.getFontHeight(), oldFont.getFontName(), oldFont.getItalic(), oldFont.getStrikeout(), oldFont.getTypeOffset(), oldFont.getUnderline());
                    if (newFont == null) {
                        newFont = newCell.getSheet().getWorkbook().createFont();
                        newFont.setBold(oldFont.getBold());
                        newFont.setColor(oldFont.getColor());
                        newFont.setFontHeight(oldFont.getFontHeight());
                        newFont.setFontName(oldFont.getFontName());
                        newFont.setItalic(oldFont.getItalic());
                        newFont.setStrikeout(oldFont.getStrikeout());
                        newFont.setTypeOffset(oldFont.getTypeOffset());
                        newFont.setUnderline(oldFont.getUnderline());
                        newFont.setCharSet(oldFont.getCharSet());
                    }

                    short newFormat = newDataFormat.getFormat(oldCell.getCellStyle().getDataFormatString());
                    newCellStyle = newCell.getSheet().getWorkbook().createCellStyle();
                    newCellStyle.setFont(newFont);
                    newCellStyle.setDataFormat(newFormat);

                    newCellStyle.setAlignment(oldCell.getCellStyle().getAlignment());
                    newCellStyle.setHidden(oldCell.getCellStyle().getHidden());
                    newCellStyle.setLocked(oldCell.getCellStyle().getLocked());
                    newCellStyle.setWrapText(oldCell.getCellStyle().getWrapText());
                    newCellStyle.setBorderBottom(oldCell.getCellStyle().getBorderBottom());
                    newCellStyle.setBorderLeft(oldCell.getCellStyle().getBorderLeft());
                    newCellStyle.setBorderRight(oldCell.getCellStyle().getBorderRight());
                    newCellStyle.setBorderTop(oldCell.getCellStyle().getBorderTop());
                    newCellStyle.setBottomBorderColor(oldCell.getCellStyle().getBottomBorderColor());
                    newCellStyle.setFillBackgroundColor(oldCell.getCellStyle().getFillBackgroundColor());
                    newCellStyle.setFillForegroundColor(oldCell.getCellStyle().getFillForegroundColor());
                    newCellStyle.setFillPattern(oldCell.getCellStyle().getFillPattern());
                    newCellStyle.setIndention(oldCell.getCellStyle().getIndention());
                    newCellStyle.setLeftBorderColor(oldCell.getCellStyle().getLeftBorderColor());
                    newCellStyle.setRightBorderColor(oldCell.getCellStyle().getRightBorderColor());
                    newCellStyle.setRotation(oldCell.getCellStyle().getRotation());
                    newCellStyle.setTopBorderColor(oldCell.getCellStyle().getTopBorderColor());
                    newCellStyle.setVerticalAlignment(oldCell.getCellStyle().getVerticalAlignment());

                    styleList.add(newCellStyle);
                }
                newCell.setCellStyle(newCellStyle);
            }
        }
        switch (oldCell.getCellType()) {
            case STRING:
                newCell.setCellValue(oldCell.getStringCellValue());
                break;
            case NUMERIC:
                newCell.setCellValue(oldCell.getNumericCellValue());
                break;
            case BLANK:
                newCell.setCellValue((String)null);
                break;
            case BOOLEAN:
                newCell.setCellValue(oldCell.getBooleanCellValue());
                break;
            case ERROR:
                newCell.setCellErrorValue(oldCell.getErrorCellValue());
                break;
            case FORMULA:
                newCell.setCellFormula(oldCell.getCellFormula());
                break;
            default:
                break;
        }

    }

    private static CellStyle getSameCellStyle(Cell oldCell, Cell newCell, List<CellStyle> styleList) {
        CellStyle styleToFind = oldCell.getCellStyle();
        CellStyle currentCellStyle = null;
        CellStyle returnCellStyle = null;
        Iterator<CellStyle> iterator = styleList.iterator();
        Font oldFont = null;
        Font newFont = null;
        while (iterator.hasNext() && returnCellStyle == null) {
            currentCellStyle = iterator.next();

            if (currentCellStyle.getAlignment() != styleToFind.getAlignment()) {
                continue;
            }
            if (currentCellStyle.getHidden() != styleToFind.getHidden()) {
                continue;
            }
            if (currentCellStyle.getLocked() != styleToFind.getLocked()) {
                continue;
            }
            if (currentCellStyle.getWrapText() != styleToFind.getWrapText()) {
                continue;
            }
            if (currentCellStyle.getBorderBottom() != styleToFind.getBorderBottom()) {
                continue;
            }
            if (currentCellStyle.getBorderLeft() != styleToFind.getBorderLeft()) {
                continue;
            }
            if (currentCellStyle.getBorderRight() != styleToFind.getBorderRight()) {
                continue;
            }
            if (currentCellStyle.getBorderTop() != styleToFind.getBorderTop()) {
                continue;
            }
            if (currentCellStyle.getBottomBorderColor() != styleToFind.getBottomBorderColor()) {
                continue;
            }
            if (currentCellStyle.getFillBackgroundColor() != styleToFind.getFillBackgroundColor()) {
                continue;
            }
            if (currentCellStyle.getFillForegroundColor() != styleToFind.getFillForegroundColor()) {
                continue;
            }
            if (currentCellStyle.getFillPattern() != styleToFind.getFillPattern()) {
                continue;
            }
            if (currentCellStyle.getIndention() != styleToFind.getIndention()) {
                continue;
            }
            if (currentCellStyle.getLeftBorderColor() != styleToFind.getLeftBorderColor()) {
                continue;
            }
            if (currentCellStyle.getRightBorderColor() != styleToFind.getRightBorderColor()) {
                continue;
            }
            if (currentCellStyle.getRotation() != styleToFind.getRotation()) {
                continue;
            }
            if (currentCellStyle.getTopBorderColor() != styleToFind.getTopBorderColor()) {
                continue;
            }
            if (currentCellStyle.getVerticalAlignment() != styleToFind.getVerticalAlignment()) {
                continue;
            }

            oldFont = oldCell.getSheet().getWorkbook().getFontAt(oldCell.getCellStyle().getFontIndex());
            newFont = newCell.getSheet().getWorkbook().getFontAt(currentCellStyle.getFontIndex());

            if (newFont.getBold() == oldFont.getBold()) {
                continue;
            }
            if (newFont.getColor() == oldFont.getColor()) {
                continue;
            }
            if (newFont.getFontHeight() == oldFont.getFontHeight()) {
                continue;
            }
            if (newFont.getFontName() == oldFont.getFontName()) {
                continue;
            }
            if (newFont.getItalic() == oldFont.getItalic()) {
                continue;
            }
            if (newFont.getStrikeout() == oldFont.getStrikeout()) {
                continue;
            }
            if (newFont.getTypeOffset() == oldFont.getTypeOffset()) {
                continue;
            }
            if (newFont.getUnderline() == oldFont.getUnderline()) {
                continue;
            }
            if (newFont.getCharSet() == oldFont.getCharSet()) {
                continue;
            }
            if (oldCell.getCellStyle().getDataFormatString().equals(currentCellStyle.getDataFormatString())) {
                continue;
            }

            returnCellStyle = currentCellStyle;
        }
        return returnCellStyle;
    }

}

