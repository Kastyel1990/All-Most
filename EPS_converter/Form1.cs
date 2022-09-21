using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using ZXing;
using ImageMagick;

namespace PicToCSV_converter
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }
        public string filename;
        private void button1_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog folderIn = new FolderBrowserDialog();
            folderIn.ShowNewFolderButton = false;
            DialogResult resultIn = folderIn.ShowDialog();
            if (resultIn == DialogResult.OK)
            {
                textBox1.Text = folderIn.SelectedPath;
                get_local_filelist(textBox1.Text);
            }
        }

        private void button3_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog folderOut = new FolderBrowserDialog();
            folderOut.ShowNewFolderButton = true;
            DialogResult resultout = folderOut.ShowDialog();
            if (resultout == DialogResult.OK)
                textBox2.Text = folderOut.SelectedPath;

        }

        private void button2_Click(object sender, EventArgs e)
        {
            if (new[] { "JPG", "PNG", "BMP", "CSV" }.Any(c => comboBox1.Text.Contains(c)) != true)
            {
                MessageBox.Show("Я не знаю во что конвертировать", "Не указан формат конечного файла!", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            if (textBox1.Text.Contains(@":\") != true)
            {
                MessageBox.Show("Я не знаю откуда брать файлы", "Не указан исходный путь!", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            if (textBox2.Text.Contains(@":\") != true)
            {
                MessageBox.Show("Я не знаю куда ложить файлы", "Не указан конечный путь!", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            if (comboBox1.Text == "CSV")
            {
                var temp_path = $@"{Application.StartupPath}\tmp";
                eps_to_pic(textBox1.Text, temp_path);
                pic_to_csv(temp_path, textBox2.Text);

                foreach (FileInfo file in new DirectoryInfo(temp_path).GetFiles())
                {
                    file.Delete();
                }
            }
            else
            {
                eps_to_pic(textBox1.Text, textBox2.Text);
            }
            
            
            MessageBox.Show("Конвертация завершена.", "Успех!!!", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        private void eps_to_pic(string in_path, string out_path)
        {
            var files = Directory.GetFiles(in_path + @"\", "*.*", SearchOption.AllDirectories).Where(s => s.EndsWith(".eps"));
            MagickNET.SetGhostscriptDirectory($@"{Application.StartupPath}");
            var settings = new MagickReadSettings();
            settings.Width = 600;
            settings.Height = 600;
            settings.Format = MagickFormat.Eps;
            settings.Density = new Density(600, 600);


            var filecount = files.Count();
            progressBar1.Value = 0;
            progressBar1.Maximum = filecount;


            try
            {
                foreach (var file in files)
                {
                    //MessageBox.Show(file);
                    string filename = file.Substring(file.LastIndexOf(@"\") + 1).Substring(0, file.Substring(file.LastIndexOf(@"\") + 1).LastIndexOf(@"."));

                    using (var image = new MagickImage(file, settings))
                    {
                        switch (comboBox1.Text)
                        {
                            case "JPG":
                                image.Format = MagickFormat.Jpg;
                                break;
                            case "PNG":
                                image.Format = MagickFormat.Png;
                                break;
                            case "BMP":
                                image.Format = MagickFormat.Bmp;
                                break;
                            case "CSV":
                                image.Format = MagickFormat.Jpg;
                                break;
                        }
                        if (comboBox1.Text == "CSV")
                            image.Write($@"{out_path}\{filename}.jpg");
                        else
                            image.Write($@"{out_path}\{filename}.{comboBox1.Text.ToLower()}");
                    }

                    progressBar1.Value += 1;
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show($@"{ex.ToString()}", "Ошибка", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void get_local_filelist(string path)
        {
            listView1.Clear();
            listView1.Items.Add("...", 2);
            path = path + @"\";
            foreach (var i in Directory.GetDirectories(path))
                listView1.Items.Add(i.Substring(i.LastIndexOf(@"\") + 1), 1);
            foreach (var i in Directory.GetFiles(path))
                listView1.Items.Add(i.Substring(i.LastIndexOf(@"\") + 1), 0);
        }

        private void pic_to_csv(string in_path, string out_path)
        {
            filename = in_path.Substring(in_path.LastIndexOf(@"\") + 1, in_path.Length - (in_path.LastIndexOf(@"\") + 1));
            StreamWriter sw = new StreamWriter($@"{out_path}\{filename}.csv");
            var files = Directory.GetFiles(in_path + @"\", "*.*", SearchOption.AllDirectories).Where(s => s.EndsWith(".jpg")
                                                                                                             || s.EndsWith(".png")
                                                                                                             || s.EndsWith(".bmp")
                                                                                                             || s.EndsWith(".gif"));
            var filecount = files.Count();
            progressBar1.Value = 0;
            progressBar1.Maximum = filecount;

            try
            {
                foreach (var file in files)
                {
                    BarcodeReader reader = new BarcodeReader
                    {
                        AutoRotate = true,

                        Options =
                        {
                        PossibleFormats = new List<BarcodeFormat>{BarcodeFormat.DATA_MATRIX},
                        TryHarder = true,
                        ReturnCodabarStartEnd = true,
                        PureBarcode = false
                        }
                    };
                    Bitmap bitmap = (Bitmap)Image.FromFile(file);
                    var barcode = reader.Decode(bitmap);
                    bitmap.Dispose();
                    sw.WriteLine($"{barcode.ToString()}");
                    progressBar1.Value += 1;

                };
                sw.Close();
                get_local_filelist(in_path);
            }
            catch (Exception ex)
            {
                MessageBox.Show($@"{ex.ToString()}", "Ошибка", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }


        }
    }
}
