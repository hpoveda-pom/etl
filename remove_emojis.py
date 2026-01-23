#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script temporal para quitar emojis de todos los archivos ETL
"""
import os
import re

# Mapeo de emojis a reemplazos
EMOJI_REPLACEMENTS = {
    '✅': '[OK]',
    '❌': '[ERROR]',
    '⚠️': '[WARN]',
    '💡': '[INFO]',
    '🔍': '',
    '🔄': '',
    '📊': '',
    '📤': '',
    '📋': '',
    '🔢': '',
    '🚫': '',
    '→': '->',
    '✓': '[OK]',
    '⏭️': '[SKIP]',
    '📥': '',
    '🔑': '',
    '⏱️': '',
    '⭐': '',
}

def remove_emojis_from_file(filepath):
    """Quita emojis de un archivo"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Reemplazar cada emoji
        for emoji, replacement in EMOJI_REPLACEMENTS.items():
            content = content.replace(emoji, replacement)
        
        # Solo escribir si hubo cambios
        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False
    except Exception as e:
        print(f"Error procesando {filepath}: {e}")
        return False

def main():
    """Procesa todos los archivos .py y README.md"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    files_to_process = []
    
    # Agregar todos los .py
    for filename in os.listdir(script_dir):
        if filename.endswith('.py') and filename != 'remove_emojis.py':
            files_to_process.append(os.path.join(script_dir, filename))
    
    # Agregar README.md
    readme_path = os.path.join(script_dir, 'README.md')
    if os.path.exists(readme_path):
        files_to_process.append(readme_path)
    
    print(f"Procesando {len(files_to_process)} archivos...")
    
    changed_count = 0
    for filepath in files_to_process:
        if remove_emojis_from_file(filepath):
            print(f"  [OK] {os.path.basename(filepath)}")
            changed_count += 1
        else:
            print(f"  [SKIP] {os.path.basename(filepath)} (sin cambios)")
    
    print(f"\nCompletado: {changed_count} archivos modificados")
    
    # Auto-eliminarse
    try:
        os.remove(__file__)
        print(f"[OK] Script temporal eliminado")
    except:
        pass

if __name__ == '__main__':
    main()
