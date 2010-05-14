﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	internal class UTF8TypeConverter : TypeConverter
	{
		public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
		{
			if (Type.GetTypeCode(sourceType) != TypeCode.Object)
				return true;

			return sourceType == typeof(byte[]);
		}

		public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
		{
			if (Type.GetTypeCode(destinationType) != TypeCode.Object)
				return true;

			return destinationType == typeof(byte[]);
		}

		public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
		{
			if (value is byte[])
				return Encoding.UTF8.GetString((byte[])value);

			return Convert.ChangeType(value, typeof(string));
		}

		public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
		{
			if (!(value is string))
				return null;

			if (destinationType == typeof(byte[]))
				return Encoding.UTF8.GetBytes((string)value);

			return Convert.ChangeType(value, destinationType);
		}
	}
}