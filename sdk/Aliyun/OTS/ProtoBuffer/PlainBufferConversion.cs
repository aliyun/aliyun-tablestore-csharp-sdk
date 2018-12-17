using System.Collections.Generic;
using System.IO;
using Aliyun.OTS.DataModel;
namespace com.alicloud.openservices.tablestore.core.protocol
{
    public static class PlainBufferConversion
    {
        public static IRow ToRow(PlainBufferRow plainBufferRow)
        {
            if (plainBufferRow.HasDeleteMarker())
            {
                throw new IOException("Row could not has delete marker: " + plainBufferRow);
            }

            if (plainBufferRow.GetPrimaryKey() == null)
            {
                throw new IOException("Row has no primary key: " + plainBufferRow);
            }

            List<Column> columns = new List<Column>(plainBufferRow.GetCells().Count);

            foreach (PlainBufferCell cell in plainBufferRow.GetCells())
            {
                columns.Add(ToColumn(cell));
            }

            return new Row(ToPrimaryKey(plainBufferRow.GetPrimaryKey()), columns);
        }

        public static Column ToColumn(PlainBufferCell cell)
        {
            if (!cell.HasCellName() || !cell.HasCellValue())
            {
                throw new IOException("The cell has no name or value: " + cell);
            }

            if (cell.HasCellType() && cell.GetCellType() != PlainBufferConsts.INCREMENT)
            {
                throw new IOException("The cell should not has type: " + cell);
            }

            if (cell.HasCellTimestamp())
            {
                return new Column(cell.GetCellName(), cell.GetCellValue(), cell.GetCellTimestamp());
            }
            else
            {
                return new Column(cell.GetCellName(), cell.GetCellValue());
            }
        }

        public static PrimaryKey ToPrimaryKey(List<PlainBufferCell> pkCells)
        {
            var primaryKey = new PrimaryKey();
            foreach (PlainBufferCell cell in pkCells)
            {
                primaryKey.Add(cell.GetCellName(), cell.GetCellValue());
            }

            return primaryKey;
        }

        public static PlainBufferCell ToPlainBufferCell(PrimaryKeyColumn primaryKeyColumn)
        {
            PlainBufferCell cell = new PlainBufferCell();
            cell.SetCellName(primaryKeyColumn.Name);
            cell.SetPkCellValue(primaryKeyColumn.Value);
            return cell;
        }

        public static PlainBufferCell ToPlainBufferCell(Column column, bool ignoreValue, bool ignoreTs,
                                                        bool setType, byte type)
        {
            PlainBufferCell cell = new PlainBufferCell();
            cell.SetCellName(column.Name);
            if (!ignoreValue)
            {
                cell.SetCellValue(column.Value);
            }
            if (!ignoreTs)
            {
                if (column.Timestamp.HasValue)
                {
                    cell.SetCellTimestamp(column.Timestamp.Value);
                }
            }
            if (setType)
            {
                cell.SetCellType(type);
            }

            return cell;
        }
    }
}
